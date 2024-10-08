# Copyright (c) 2023 Guillaume Outters
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

case "$_UTIL_MUSQLE_SH_" in "")

#- Environnement ---------------------------------------------------------------

configBdd()
{
	BDD="$1"
	
	# Historiquement oraParams a été montée avant configBdd, on l'intègre donc.
	if command -v oraParams > /dev/null 2>&1
	then
		oraParams "$@" && BDD_TYPE=oracle && return 0 || true
	fi
	
	echo "# Configuration introuvable pour la base '$1'" >&2
	return 1
}

denicherMUSQLE()
{
	_trouver()
	{
		unset IFS
		local f quoi="$1" ; shift
		for f in "$@"
		do
			case "$f" in */$quoi) MUSQLE="`dirname "$f"`" ; return ;; esac
		done
	}
	
	# À FAIRE: exploiter $BASH_SOURCE quand on est lancé par lui.
	case "$LOMBRICPATH:" in
		*/util.oracle.sh:*) IFS=: ; _trouver util.oracle.sh $LOMBRICPATH ;;
	esac
}

denicherMUSQLE

#- Utilitaires -----------------------------------------------------------------

miamParam()
{
	case "$2" in "") return 1 ;; esac
	eval "$2=\$1" ; shift 2
	params="$*"
}

tifs()
{
	unset IFS
	"$@"
}

commande()
{
	command -v "$*" 2> /dev/null >&2
}

# Utilisation: dernier <var> 1 2 3 … n
# la variable <var> aura en sortie la valeur "n"
dernier()
{
	unset IFS
	eval $1'="$'$#\"
}

# Utilisation: paire <var1> <var2> 1 2 3 … n
# <var1> aura en sortie la valeur "1", <var2> vaudra "2 3 … n"
# N.B.: "paire" signifie Premier Argument à Isoler du Reste par Extraction
paire()
{
	unset IFS
	eval $1='"$3"'
	local var2="$2"
	shift 3 || true
	eval $var2='"$*"'
}

#- Transferts ------------------------------------------------------------------

# Exécute une extraction sur une table et la restitue sous forme d'un create table pour une autre base, à passer dans un sql2csv.php
# Se contenter de quelques milliers d'entrées (pour plus, se constituer des extracteurs maison dédiés).
sql2table()
{
	local base sep=';' sepl= table crea temp params=table T=/tmp/temp.sql2table.$$
	
	while [ $# -gt 0 ]
	do
		case "$1" in
			-b) base="$2" ; shift ;;
			-s) sep="$2" ; shift ;;
			-l) sepl="$2" ; shift ;;
			-t|temp) crea=1 ; temp=-t ;;
			-c) crea=1 ;;
			*) miamParam "$1" $params || break ;;
		esac
		shift
	done
	if [ -z "$*" -o -z "$table" ]
	then
		cat >&2 <<TERMINE
# Utilisation: sql2table [-s <sép>] [-l <sép ligne>] [-b <base>] [temp|-t|-c] <tablea>[:<type>] <fichier .sql>|<requête sql>|-
  -s <sép>
  -l <sép ligne>
    (sous réserve de support par le moteur sous-jacent)
    La dernière ligne elle aussi doit être terminée par cette chaîne.
  -b <base>
  [temp|-t|-c] <table>[:<type>]
    Nom de la table portant le résultat côté à la cible.
    Le mot-clé "temp" ou l'option -t la crée temporaire.
    L'option "-c" la crée tout bonnement.
    Sans option elle est juste alimentée, supputée déjà créée (mais alors sql2table perd de son intérêt, de produire la requête de création).
    Si <type> est précisée, un format optimisé pour ce type de base pourra être généré.
  <fichier .sql>|<requête sql>|-
    Requête d'extraction à jouer côté Oracle, ou fichier contenant le SQL.
    - désigne l'entrée standard.
TERMINE
		return 1
	fi
	
	# Extraction
	
	configBdd "$base" || return 1
	
	case "$BDD_TYPE" in
		"") echo "# La config de base n'a pas typé '$base'." >&2 ; return 1 ;;
	esac
	${BDD_TYPE}_extraire -s "$sep" "$T.descr" "$T.csv" "$@" >&2 # _extraire étant censé pondre vers notre CSV, tout ce qui n'y tombe pas est probablement de la pollution qui va retomber sur notre pauvre appelant (croyant qu'il s'agit d'une ligne CSV à interpréter).
	
	# Import
	
	local typeCible csvVersSql sepop="`printf '\014'`" options
	case "$table" in
		*:*) IFS=: ; paire table typeCible $table ;;
	esac
	[ -z "$crea" ] || creaVersSql $temp "$table" "$T.descr"
	case "$sepl" in ?*) options="$options-l$sepop$sepl$sepop" ;; esac
	IFS="$sepop"
	tifs csvVersSql -s "$sep" $options "$table" "$T.descr" "$T.csv"
	
	# À FAIRE: implémenter aussi l'intégration vers une nouvelle base, par exemple avec une option -d <base destination>,
	#          qui permettrait d'avoir un outil tout-en-un pouvant faire office d'ETL.
	#          Attention, cette option serait incompatible avec le -t (qui génère une table temporaire), puisque la table temp disparaîtrait aussitôt terminé l'import.
	
	rm $T.*
}

csv2table()
{
	local sep=';' sepl crea=0
	while [ $# -gt 0 ]
	do
		case "$1" in
			-s) sep="$2" ; shift ;;
			-l) sepl="$2" ; shift ;;
			-b) configBdd "$2" ; shift ;;
			--drop) crea=-1 ;;
			-c) crea=1 ;;
			*) break;
		esac
		shift
	done
	
	local table="$1" descr="$2" csv="$3" options
	
	# Création.
	
	case "$crea" in 1|-1)
		{
			# Il est demandé une destruction préalable de la table destination.
			case "$crea" in -1)
				echo "drop table if exists $table;" ;;
			esac
			creaVersSql "$table:$BDD_TYPE" "$descr"
		} | sql$BDD_TYPE
		;;
	esac
	
	# Remplissage.
	
	local sepop="`printf '\014'`" options
	case "$sepl" in ?*) options="$options-l$sepop$sepl$sepop" ;; esac
	
	IFS="$sepop"
	tifs csvVersTable -s "$sep" $options "$table" "$descr" "$csv"
}

creaVersSql()
{
	local temp typeCible
	case "$1" in -t) shift ; temp=temporary ;; esac
	local table="$1" descr="$2"
	case "$table" in *:*) IFS=: ; paire table typeCible $table ;; esac
	local filtre=_creaVersSql_$typeCible
	commande $filtre || filtre=cat
	
	echo "create $temp table $table"
	echo "("
	sed -e 's/$/,/' -e 's/,,$/,/' -e `wc -l < "$descr"`'s/,$//' < "$descr" | $filtre
	echo ");"
}

# Convertit un ensemble <table> <fichier de description des colonnes> <CSV> en instructions SQL.
csvVersSql()
{
	local BDD_TYPE="$BDD_TYPE"
	case "$typeCible" in ?*) BDD_TYPE="$typeCible" ;; esac
	
	# Recherche d'une implémentation spécifique.
	
	local specifique="${BDD_TYPE}_csvVersSql"
	if commande $specifique
	then
		$specifique "$@" || return $?
		return
	fi
	
	# Implémentation générique.
	
	# À FAIRE: un gros bidule à coup d'insert.
	echo "# csvVersSql() n'a pas d'implémentation générique. Désolé." >&2
	return 1
}

pgsql_csvVersSql() { sqleurcopy_csvVersSql "$@" ; }
sqleurcopy_csvVersSql()
{
	local sep=';'
	case "$1" in -s) sep="$2" ; shift 2 ;; esac
	local table="$1" descr="$2" csv="$3"
	echo "#prepro copy"
	sed < "$csv" \
		-e "1{
s/$sep/,/g
s/$/) csv delimiter '$sep' from stdin/
s/^/#copy $table (/
a\\
\$ora2pg\$
}" \
		-e 's/<[^>]*>//g' \
		-e '$a\
$ora2pg$;
'
}

# Pousse un ensemble <table> <fichier de description des colonnes> <CSV> vers une table.
csvVersTable()
{
	local BDD_TYPE="$BDD_TYPE"
	case "$typeCible" in ?*) BDD_TYPE="$typeCible" ;; esac
	
	# Recherche d'une implémentation spécifique.
	
	local specifique="${BDD_TYPE}_csvVersTable"
	if commande $specifique
	then
		$specifique "$@" || return $?
		return
	fi
	
	# Implémentation générique.
	
	csvVersSql "$@" | sql$BDD_TYPE
}

_UTIL_MUSQLE_SH_=1 ;; esac
