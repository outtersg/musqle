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
	# Historiquement oraParams a été montée avant configBdd, on l'intègre donc.
	if command -v oraParams > /dev/null 2>&1
	then
		oraParams "$@" && BDD_TYPE=oracle && return 0 || true
	fi
	
	echo "# Configuration introuvable pour la base '$1'" >&2
	return 1
}

#- Utilitaires -----------------------------------------------------------------

miamParam()
{
	case "$2" in "") return 1 ;; esac
	eval "$2=\$1" ; shift 2
	params="$*"
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
	local base sep=';' table crea temp params=table T=/tmp/temp.sql2table.$$
	
	while [ $# -gt 0 ]
	do
		case "$1" in
			-b) base="$2" ; shift ;;
			-s) sep="$2" ; shift ;;
			-t|temp) crea=1 ; temp=-t ;;
			-c) crea=1 ;;
			*) miamParam "$1" $params || break ;;
		esac
		shift
	done
	if [ -z "$*" -o -z "$table" ]
	then
		cat >&2 <<TERMINE
# Utilisation: sql2table [-s <sép>] [-b <base>] [temp|-t|-c] <tablea>[:<type>] <fichier .sql>|<requête sql>
  -s <sép>
  -b <base>
  [temp|-t|-c] <table>[:<type>]
    Nom de la table portant le résultat côté à la cible.
    Le mot-clé "temp" ou l'option -t la crée temporaire.
    L'option "-c" la crée tout bonnement.
    Sans option elle est juste alimentée, supputée déjà créée (mais alors sql2table perd de son intérêt, de produire la requête de création).
    Si <type> est précisée, un format optimisé pour ce type de base pourra être généré.
  <fichier .sql>|<requête sql>
    Requête d'extraction à jouer côté Oracle, ou fichier contenant le SQL.
TERMINE
		return 1
	fi
	
	# Extraction
	
	configBdd "$base" || return 1
	
	case "$BDD_TYPE" in
		"") echo "# La config de base n'a pas typé '$base'." >&2 ; return 1 ;;
	esac
	${BDD_TYPE}_extraire "$T.descr" "$T.csv" "$@" >&2 # _extraire étant censé pondre vers notre CSV, tout ce qui n'y tombe pas est probablement de la pollution qui va retomber sur notre pauvre appelant (croyant qu'il s'agit d'une ligne CSV à interpréter).
	
	# Import
	
	local typeCible csvVersSql
	case "$table" in
		*:*) IFS=: ; paire table typeCible $table ;;
	esac
	[ -z "$crea" ] || creaVersSql $temp "$table" "$T.descr"
	for csvVersSql in "${typeCible}_" ""
	do
		csvVersSql="${csvVersSql}csvVersSql"
		commande $csvVersSql && break
		break
	done
	$csvVersSql -s "$sep" "$table" "$T.descr" "$T.csv"
	
	# À FAIRE: implémenter aussi l'intégration vers une nouvelle base, par exemple avec une option -d <base destination>,
	#          qui permettrait d'avoir un outil tout-en-un pouvant faire office d'ETL.
	#          Attention, cette option serait incompatible avec le -t (qui génère une table temporaire), puisque la table temp disparaîtrait aussitôt terminé l'import.
	
	rm $T.*
}

creaVersSql()
{
	local temp
	case "$1" in -t) shift ; temp=temporary ;; esac
	local table="$1" descr="$2"
	echo "create $temp table $table"
	echo "("
	sed -e 's/$/,/' -e 's/,,$/,/' -e `wc -l < "$descr"`'s/,$//' < "$descr"
	echo ");"
}

# Convertit un ensemble <fichier de description des colonnes> <CSV> <table> en instructions SQL.
# Repose sur les variables:
# - $table Table à alimenter.
csvVersSql()
{
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
s/$/) delimiter '$sep' from stdin/
s/^/#copy $table (/
a\\
\$ora2pg\$
}" \
		-e '$a\
$ora2pg$;
'
}

_UTIL_MUSQLE_SH_=1 ;; esac
