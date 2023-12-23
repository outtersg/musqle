# Copyright (c) 2021-2023 Guillaume Outters
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

# À inclure depuis votre .bashrc ou autre.
# Pensez à définir $BDD_NOM, $BDD_QUI, $BDD_MDP.

case "$_UTIL_ORACLE_SH_" in "")

BDD_SQLEUR=sqloracle

oraParams()
{
	if [ "x$BDD_IM" = "x$BDD_IM_calcule" ] ; then BDD_IM= ; fi
	if [ -z "$BDD_IM" -a -n "$BDD_QUI" -a -n "$BDD_MDP" ] ; then BDD_IM="$BDD_QUI/$BDD_MDP" ; fi
	[ -n "$BDD_HOTE" ] || BDD_HOTE=localhost
	[ -n "$BDD_PORT" ] || BDD_PORT=1521
	
	if [ -z "$BDD_IM" -o -z "$BDD_NOM" ]
	then
		echo "# Veuillez définir les variables \$BDD_IM (ou \$BDD_QUI + \$BDD_MDP) et \$BDD_NOM" >&2
		return 1
	fi
	
	_oraParams_chaine "$@"
}

_oraParams_chaine()
{
	case "$1" in
		complexe) BDD_CHAINE="$BDD_IM@`_oraParams_chaineMulti "$BDD_HOTE $BDD_HOTE2 $BDD_HOTE3 $BDD_HOTE4" "$BDD_PORT" "$BDD_NOM"`" ;;
		*) BDD_CHAINE="$BDD_IM@$BDD_HOTE:$BDD_PORT:$BDD_NOM" ;;
	esac
}

_oraParams_chaineMulti()
{
	local vars="hotes port base" var hotes= hote port= base=
	# À FAIRE: distinguer nom de service et SID.
	# À FAIRE: options
	while [ $# -gt 0 ]
	do
		case "$1" in
			*)
				[ -n "$vars" ] || { echo "# _oraParams_chaineMulti(): paramètre \"$1\" surnuméraire." >&2 ; return 1 ; }
				_oraParams_chaineMulti_var "$1" $vars
				;;
		esac
		shift
	done
	printf "(DESCRIPTION=(CONNECT_TIMEOUT=3)(RETRY_COUNT=2)"
	case "$hotes" in
		*[^\ ]*[\ ][^\ ]*) printf "(FAILOVER=ON)(LOAD_BALANCE=NO)" ;;
	esac
	for hote in $hotes
	do
		printf "(ADDRESS=(PROTOCOL=TCP)(HOST=$hote)(PORT=$port))"
	done
	printf "(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=$base)))"
}

_oraParams_chaineMulti_var()
{
	eval $2='"$1"'
	shift ; shift
	vars="$*"
}

sqloracle()
{
	# À FAIRE: basculer sur du oraParams()
	[ -n "$BDD_MDP" ] || { echo "# Veuillez définir la variable \$BDD_MDP." >&2 ; return 1 ; }
	[ -n "$BDD_HOTE" ] || BDD_HOTE=localhost
	[ -n "$BDD_PORT" ] || BDD_PORT=1521
	
	sqlplus -s $BDD_QUI/$BDD_MDP@"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=$BDD_HOTE)(PORT=$BDD_PORT))(CONNECT_DATA=(SID=$BDD_NOM)))"
}

miamParam()
{
	case "$2" in "") return 1 ;; esac
	eval "$2=\$1" ; shift 2
	params="$*"
}

#- Transferts ------------------------------------------------------------------

oraCopy()
{
	local fc=/tmp/temp.oraCopy.$$ # Fichiers de Contrôle.
	local params="csv table" csv base table sep=";" optionsSqlldr="log=\"$fc.log\", direct=true"
	
	while [ $# -gt 0 ]
	do
		case "$1" in
			-1) optionsSqlldr="$optionsSqlldr, skip=1" ;;
			-b) base="$2" ; shift ;;
			-s) sep="$2" ; case "$sep" in \\[t]|\\[0-9][0-9][0-9]) sep="`printf "$sep"`" ;; esac ; shift ;;
			*)
				case "$params" in "") break ;; esac # Plus de param à renseigner? C'est qu'on arrive à la première colonne.
				miamParam "$1" $params
				;;
		esac
		shift
	done
	if [ -z "$*" -o ! -f "$csv" ]
	then
		echo "# Utilisation: oraCopy [-1] [-s <sép>] <csv> [-b <base>] <table> <colonne>+" >&2
		return 1
	fi
	
	local cols="`IFS=, ; echo "$*"`"
	
	oraParams "$base" || return 1
	
	{
		# À FAIRE: peut-on ne pas préciser pas ($cols)?
		cat <<TERMINE
options ($optionsSqlldr)
load data
badfile "$fc.bad"
append into table $table
fields terminated by '$sep'
trailing nullcols
($cols)
TERMINE
	} > $fc.ctl
	
	# /!\ $BDD_NOM doit avoir été déclaré dans le tnsnames.ora,
	#     car (en SSH vers une 11.2 en tout cas) le userid ne peut être passé sous sa forme longue).
	#local chaineCo="$BDD_IM@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=$BDD_HOTE)(PORT=$BDD_PORT))(CONNECT_DATA=(SID=$BDD_NOM)))"
	
	case "$BDD_SSH" in
		""|localhost)
			[ -z "$BDD_ENV" ] || eval "$BDD_ENV"
			sqlldr userid="$BDD_IM@$BDD_NOM" control=$fc.ctl data="$csv" < /dev/null
			;;
		*)
			scp -C $fc.ctl "$csv" $BDD_SSH:/tmp/
			ssh $BDD_SSH "$BDD_ENV ; sqlldr userid=$BDD_IM@$BDD_NOM control=$fc.ctl data=/tmp/`basename "$csv"` && rm -f $fc.ctl $fc.bad $fc.log $csvd" < /dev/null
			;;
	esac
}

# Exécute une extraction Oracle et la restitue sous forme d'un create temp table PostgreSQL, à passer dans un sql2csv.php
# /!\ Repose sur l'inclusion de ../sqleur/sqlminus.sh
oracle_extraire()
{
	local sep=';'
	while [ $# -gt 0 ]
	do
		case "$1" in
			-s) sep="$2" ; shift ;;
			*) break ;;
		esac
		shift
	done
	
	# Le fichier de description demandé par l'appelant n'a pas le même format (SQL) que celui produit par sqlm (bidule Oracle).
	# On écarte donc ce paramètre, et on traduira.
	local descr="$1" ; shift
	local T="`echo "$1" | sed -e 's/\.csv$//'`"
	
	command -v sqlm 2> /dev/null >&2 || [ ! -f "$SCRIPTS/../sqleur/sqlminus.sh" ] || . "$SCRIPTS/../sqleur/sqlminus.sh"
	sqlm --null NULL -s "$sep" -o "$@"
	
	case "$BDD_SSH" in
		""|localhost) true ;;
		*)
			scp -q -C "$BDD_SSH:$T.*" /tmp/
			ssh "$BDD_SSH" rm "$T.*"
			;;
	esac
	
	_ora2pg > "$descr"
}

# À FAIRE: dans sqleur, un machin qui permette d'invoquer directement ora2pg, par exemple sous la forme:
#   #create temp table t_bla from base_ext as 3
#     drop table if exists t_extraction_bla;
#     create table t_extraction_bla_intermediaire as select …;
#     select i.*, d.x, d.y from t_extraction_bla_intermediaire i join donnees using(id);
# (base_ext étant reconnue par oraParams).
# 
# Voire même, si une seule requête suffit de l'autre côté à extraire le jeu de données:
#   #set IDS_À_EXTRAIRE `select id from t_ids_a_recupere`
#   create temp table t_bla from base_ext as
#     select i.*, d.x, d.y from donnees where ORACLE_IN(id, IDS_À_EXTRAIRE);
# (par des #define dynamiques sur /create temp table [^ ]* from/ et ORACLE_IN() pour faire le découpage en lignes de moins de 1000 éléments et 4000 caractères)
# 
# create temp table 

_ora2pg()
{
		sed -E < $T.ctl \
			-e '1,/^\(/d' \
			-e '/^\)/,$d' \
			-e 's/^[^"]*"/  /' \
			-e 's/".*(INTEGER|FLOAT|TIMESTAMP|TIMESTAMPTZ|DATE).*[^,](,*)$/ \1\2/' \
			-e 's/".*[^,](,*)$/ text\1/'
}

#- Quelle Table ----------------------------------------------------------------

QT_N=4
# Dans quelle table figure tel RowID?
# https://chartio.com/resources/tutorials/how-to-list-all-tables-in-oracle/
quelletable()
{
	# À FAIRE: si des stats ont tourné, classer par durée d'exécution décroissante (les inconnues en premier).
	T=/tmp/temp.qt.$$
	
	STATS="and num_rows > 0"
	
	GREP="stdbuf -oL grep"
	EGREP="stdbuf -oL egrep"
	SED="stdbuf -oL sed"
	
	groui() { $EGREP '^oui|^[=?] ' ; }
	grouin() { $GREP '^[=?1-9]' ; }
	saufCertaines() { egrep -v "$SAUF" ; }
	
	QUOI="'oui', row_id"
	GROUI=groui
	FILTRE_TABLES=cat
	
	c=row_id
	
	while [ $# -gt 0 ]
	do
		case "$1" in
			# -a: récupère aussi les tables sans statistiques.
			-a) STATS="and (num_rows is null or num_rows > 0)" ;;
			-n) QUOI="count(1)" ; GROUI=grouin ;;
			-x) SAUF="$2" ; FILTRE_TABLES=saufCertaines ; shift ;;
	# Si le premier paramètre ressemble à un nom de colonne, on bascule sur cette colonne.
			# Sinon on sort de la boucle (les paramètres restant sont les valeurs à rechercher).
			*[-\ ]*|[0-9]*) break ;;
			*_*|id|ID) c="$1" ; shift ; break ;;
			*) break ;;
		esac
		shift
	done
	
	guillemette() { sed -e "s/ /','/g" ; }
	trucs="`echo "$*" | guillemette`"
	
	chrono()
	{
		chrono_la="`date +%s`" # À défaut de millisecondes.
		eval "chrono_info=\"\$chrono_info_$1\" ; chrono_info_$1=\"\$2\" ; chrono_t0=\$chrono_t0_$1 ; chrono_t0_$1=$chrono_la"
		[ -z "$chrono_info" ] || echo "`expr $chrono_la - $chrono_t0` $chrono_info" >> $T.chrono
	}
	
	nFaits=-$QT_N
	if command -v incruster > /dev/null 2>&1
	then
		detailProgression=" "
	fi
	enCours()
	{
		shift
		colonne="`expr $1 + 1`" ; shift
		info="$*"
		
		if [ $colonne -gt 0 ] # S'il y a du mouvement (si c'est 0, c'est juste pour réaffichage).
		then
			nFaits=`expr $nFaits + 1`
		progression="$nFaits / $nAFaire"
			if [ -n "$detailProgression" ]
			then
				nColsAff=$((QT_N + 1))
				detailProgression="`incruster "$progression" "$detailProgression" -b 0 / $nColsAff`"
				detailProgression="`incruster -c "$info" "$detailProgression" -b $colonne / $nColsAff`"
				progression="$detailProgression"
			fi
			chrono $colonne "$info"
		fi
		printf "\\r%s" "$progression"
	}
	
	grainAMoudre()
	{
		awk '
			/^= /{
				traiteur = $2;
				if(getline < "'"$T.t"'" > 0)
					print > traiteur;
				else
				{
					close(traiteur);
					system("rm "traiteur);
				}
				next;
			}
			{print}
		'
		rm "$T.t"
		# Sinon en shell pour éviter de fermer le tube: https://stackoverflow.com/a/8436387
	}
	
	{
		cat <<TERMINE
set pagesize 0
set feedback off

select t.table_name||' '||c.column_name
from all_tables t, all_tab_columns c
where
	t.owner = '$BDD_QUI' and c.table_name = t.table_name and c.owner = t.owner and lower(column_name) like lower('`echo "$c" | sed -e 's#_#\\_#g'`') escape '\\'
	$STATS
order by t.table_name desc;
TERMINE
	} | $BDD_SQLEUR | $FILTRE_TABLES | _qttrie > $T.t
	
	# À FAIRE: au départ, indiquer que l'on commence à moudre (on ne commence à afficher que lorsque l'on a passé le cap des $QT_N tables, or maintenant que l'on commence par les plus longues avec _qttrie, l'interface semble bloquée un bon moment).
	
	nAFaire=`wc -l < $T.t`
	printf "Recherche parmi %d colonnes de %d tables\n" "$nAFaire" "`cat $T.t | cut -d ' ' -f 1 | sort -u | wc -l`" >&2
	{
		i=0
			while [ $i -lt $QT_N ]
	do
			f=$T.t.$i
			mkfifo $f
			echo "= $f" # Et on se signale une première fois comme prêts à bosser.
		{
			echo "set pagesize 0;"
			while read t c
			do
				echo "select '? $i $t.$c' from dual;"
				echo "select $QUOI, '$t.$c' from $t where $c in ('$trucs');"
				echo "select '= $f' from dual;" # "J'ai fini, quelqu'un peut me renvoyer du travail?"
			done < $f
			} | { $BDD_SQLEUR 2>&1 ; echo "? $i (FINI)" ; } | $SED -e 's/^[ 	][	 ]*//' | $GROUI &
		i=`expr $i + 1`
	done
	wait
	} | grainAMoudre | while read l
	do
		case "$l" in
			"?"*) enCours $l ;;
			*)
				# On efface notre progression et on se replace en début de ligne.
				printf "\\r"
				if [ -n "$detailProgression" ]
				then
					largeur=`expr $COLUMNS - 1`
					printf "%$largeur.${largeur}s\\r" ""
				fi
				# On affiche notre trouvaille.
				echo "$l"
				# Et on remet notre progression sur la ligne d'en-dessous.
				enCours \? -1
				;;
		esac
	done
	echo
}

# Quelle Table Sauf Grosses:
# prépare un paramètre à coller derrière un quelletable -x, pour lui demander de *ne pas* parcourir ces tables / colonnes la prochaine fois.
# Se fonde sur le diagnostic pondu par une précédente passe, donc on est obligés de le faire tourner une première fois.
qtsg()
{
	[ -n "$MOINS_X_ACTUEL" ] || MOINS_X_ACTUEL="youpitralala"
	INSOUTENABLE=400 # Nombre de secondes où vraiment ça ne sert à rien.
	
	rm -f /tmp/chrono.sqlite3
	sqlite3 /tmp/chrono.sqlite3 "create table t0 (n integer, t text, c text);"
	cat /tmp/temp.qt.*.chrono \
	| egrep -v "$MOINS_X_ACTUEL|(FINI)" \
	| sed -e "s/\\./','/" -e "s/ /,'/" -e "s/$/');/" -e "s/^/insert into t0 values (/" \
	| sqlite3 /tmp/chrono.sqlite3
	
	echo "À ajouter à un -x en premier paramètre du prochain quelletable que vous lancerez:"
	sqlite3 /tmp/chrono.sqlite3 \
	"
		create table t as select max(n) n, t, c from t0 group by 2, 3;
		with
			champs as
			(
				select distinct
					t,
					group_concat(c, '|') over fen champs,
					group_concat(n, '|') over fen nindiv
				from t
				where n > $INSOUTENABLE window fen as (partition by t order by n desc rows between unbounded preceding and unbounded following)
			),
			gros as
			(
				select sum(n), t, champs, nindiv from t join champs using(t) group by 2 order by 1 desc
			),
			exprgros as
			(
				select group_concat(t||'.('||champs||')', '|') from gros -- . et non \\. car le fichier intermédiaire utilise pour le moment un espace plutôt qu'un point.
			)
		select * from exprgros;
	"
}

# Trie les tables des plus longues aux plus rapides, si des mesures précédentes ont été effectuées.
_qttrie()
{
	# Destiné à une vieille machine, sans local, sans find -maxdepth ni sed -E, et avec un awk horriblement lent.
	_qttrie_par=sed
	_qttrie_script=/tmp/temp.qt.$$.tri.$par
	
	# Il nous faut des mesures préalables.
	#find /tmp/ -maxdepth 1 -name "temp.qt.*.chrono" | grep -q . || { cat ; return ; }
	stat /tmp/temp.qt.*.chrono 2> /dev/null >&2 || { cat ; return 0 ; }
	
	# Constitution d'un awk de tri.
	{
		# Le sort -nr permet de conserver la mesure la plus élevée pour une table.
		if [ $_qttrie_par = awk ]
		then
		cat /tmp/temp.qt.*.chrono | sort -nr | sed -e '/^\([0-9][0-9]*\) \([^ ][^ ]*\)$/!d' -e 's##/^\2$/{ print "\1 "$0; next; }#' -e 's#[.]# #'
		echo '{ print "999999 "$0; }'
		else
		cat /tmp/temp.qt.*.chrono | sort -nr | sed -e '/^\([0-9][0-9]*\) \([^ ][^ ]*\)$/!d' -e 's##/^\2$/{@s/^/\1 /@b@}#' -e 's#[.]# #' | tr @ '\012'
		echo 's/^/999999 /'
		fi
	} > $_qttrie_script
	
	$_qttrie_par -f $_qttrie_script | sort -nr | cut -d ' ' -f 2-
}

_UTIL_ORACLE_SH_=1 ;; esac
