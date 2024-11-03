# Copyright (c) 2021-2024 Guillaume Outters
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
	case "$BDD_TYPE" in ora*|oci*) true ;; *) BDD_TYPE=oracle ;; esac
	
	if [ -z "$BDD_IM" -o -z "$BDD_NOM" ]
	then
		echo "# Veuillez définir les variables \$BDD_IM (ou \$BDD_QUI + \$BDD_MDP) et \$BDD_NOM" >&2
		return 1
	fi
	
	_oraParams_chaine "$@"
}

_oraParams_chaine()
{
	case "$1:$BDD_NOM" in
		complexe:*)
			BDD_CHAINE="$BDD_IM@`_oraParams_chaineMulti "$BDD_HOTE $BDD_HOTE2 $BDD_HOTE3 $BDD_HOTE4" "$BDD_PORT" "$BDD_NOM"`"
			;;
		:/*) BDD_CHAINE="$BDD_IM@//$BDD_HOTE:$BDD_PORT$BDD_NOM" ;;
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
	case "$base" in
		/*) base="SERVICE_NAME=`basename "$base"`" ;;
		*) base="SID=$base" ;;
	esac
	printf "(CONNECT_DATA=(SERVER=DEDICATED)($base)))"
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

#- Tables ----------------------------------------------------------------------

_creaVersSql_oracle()
{
	sed -E \
		-e 's/varchar\(([4-9][0-9]{3}|[0-9]{5,})\)/clob/gi' \
		-e 's/varchar\(/varchar2(/gi' \
		-e 's/ boolean(,|$)/ char(1)\1/gi'
}

#- Index -----------------------------------------------------------------------

reindex_oracle()
{
	local base filtre age="3 months" para=8 passes="1 2 3 4" butee=/tmp/stopreindex # passes: pour gérer les échecs si l'index est en cours d'utilisation.
	while [ $# -gt 0 ]
	do
		case "$1" in
			-b) shift ; base="$1" ;;
			-n) passes= ;;
			[1-9]*" "[a-zA-Z]*) age="$1" ;;
			*) filtre="$1" ;;
		esac
		shift
	done
	_ora_duree()
	{
		case "$2" in *[sS]) set -- "$1" "`echo "$2" | sed -e 's/.$//'`" ;; esac
		echo "'$1' $2"
	}
	age="`_ora_duree $age`"
	
	oraParams "$base" complexe || return 1
	
	command -v sqlm 2> /dev/null >&2 || [ ! -f "$SCRIPTS/../sqleur/sqlminus.sh" ] || . "$SCRIPTS/../sqleur/sqlminus.sh"
	
	sqlm -s \\t \
	"
		select /*+ parallel(8) */
			table_name, index_name, coalesce(degree, '1'), logging, last_analyzed
		from all_indexes i
		where $filtre
		and last_analyzed < sysdate - interval $age
		-- Si la table contient 0 entrée, son last_analyzed restera éternellement à la date de création de l'index.
		-- On évite donc ces index.
		-- Cependant les tables trop petites restent aussi hors radar (https://stackoverflow.com/a/19390409); on regarde donc si d'autres index sur la même table ont du contenu.
		-- À FAIRE: les index conditionnels qui peuvent n'avoir aucun contenu tandis que la table a des entrées.
		and (num_rows > 0 or (unpasvide(table_name) > 0 and exists (select 1 from all_indexes i2 where i2.table_name = i.table_name and i2.num_rows > 0))) -- Oracle court-circuite le second si le premier répond.
		order by last_analyzed
	" | sed -e 1d | while read itable inom ipara ihisto idate
	do
		[ ! -f "$butee" ] || { echo "[33m# Arrêt demandé par présence d'un fichier $butee" >&2 ; break ; }
		
		echo "=== $itable.$inom ==="
		echo "Dernier calcul: $idate"
		date
		boulot="alter index $inom rebuild online parallel $para nologging;"
		case "$passes" in
			"") echo "$boulot" ;;
			*)
				for tentative in $passes
				do
					[ ! -f "$butee" ] || break
					sqlm "$boulot" && break
				done
				;;
		esac
		{
			echo "alter session set ddl_lock_timeout = 120;" # Pour éviter les ORA-00054.
			case "$ipara" in "$para") true ;; 1) echo "alter index $inom noparallel;" ;; *) echo "alter index $inom parallel $ipara;" ;; esac
			case "$ihisto" in YES) echo "alter index $inom logging;" ;; esac
		} | \
		case "$passes" in
			"") cat ;;
			*) sqlm ;;
		esac
	done
}

#- Transferts ------------------------------------------------------------------

oraCopy()
{
	# NOTE: oraCopy et champs multi-lignes
	# Pour pousser du multi-lignes vers un CLOB, on peut utiliser un séparateur d'enregistrements augmenté, par exemple un caractère de contrôle en fin de ligne.
	# Mode opératoire:
	# - à l'extraction:
	#   - terminer par un champ texte *non nul* en lui concaténant un caractère spécial. Ce peut être le champ à retours ou un autre.
	#   - ne pas inclure les en-têtes dans la sortie, car eux ne pourraient se terminer par le caractère spécial.
	#   - bien nommer les colonnes composites dont celle par concaténation du séparateur (sql2csv.php pouvant décider de fusionner les colonnes de même nom, donc si toutes s'appellent ?column?, le CSV n'aura qu'une seule colonne pour toutes et donc pas assez par rapport aux colonnes à importer).
	#   Ce qui donne:
	#     #sortie /tmp/donnees.brut
	#     #format sans-en-tête delim \037
	#     select id, champ_long, coalesce(champ_texte, '')||E'\036' champ_texte from ma_table_source;
	# - à l'import:
	#   - exploiter la possibilité de mentionner les séparateurs sous forme octale
	#   - le séparateur d'enregistrements inclut le caractère de contrôle *et* le retour le retour à la ligne final: \036\n
	#   - pour du CLOB, bien penser que sqlldr utilise par défaut un tampon étriqué pour les chaînes de caractères => y aller d'un char(<tailleMax>) bien ample
	#     Cf. https://stackoverflow.com/questions/10991229/adding-clob-column-to-oracle-database-using-sqlloader
	#   Ce qui donne:
	#     oraCopy /tmp/donnees.brut -b <base> -s '\037' -rs '\036\n' ma_table_dest id "champ_long char(999999999)" champ_texte
	# (évacuation de l'autre piste envisagée: l'optionally enclosed by '"' gère la partie double-guillemet de la spéc CSV ("<balise attr=""val""/>"), mais pas les retours à la ligne entre guillemets, car sqlldr lit ligne à ligne et finit en "second sparateur de fin manquant".
	
	# NOTE: oraCopy et date
	# Le format de date peut être accolé à la colonne sous la forme:
	#   oraCopy … ma_table colonne1 colonne2 "colonne_date date 'YYYY-MM-DD HH24:MI:SS'"
	
	# NOTE: explosion d'index
	# /!\ Veillez à respecter toutes les contraintes dans les données importées.
	# Pour des raisons de perfs, oraCopy exploite le mode direct=true, au grand détriment de l'intégrité.
	# Mais ça nous donne un import vraiment à poil: en particulier il autorise la création d'enregistrements de même clé primaire que des existants.
	# Ce n'est qu'ensuite à l'exécution qu'on se tapera des "ORA-01502: l'index '…' ou sa partition est inutilisable"; seul le truncate permettra de s'en tirer.
	# Cf. https://stackoverflow.com/a/37947714
	
	# À FAIRE?: une option sans direct=true?
	
	local fc=/tmp/temp.oraCopy.$$ # Fichiers de Contrôle.
	local params="csv table" csv base table sep=";" rs= optionsSqlldr="direct=true"
	local paramsSqlldr="control=$fc.ctl log=$fc.log silent=all" # le silent=all n'a aucun effet dans le fichier de contrôle, on passe par la ligne de commande. De même pour le log dans les options (cf. https://stackoverflow.com/a/14277555).
	
	while [ $# -gt 0 ]
	do
		case "$1" in
			-1) optionsSqlldr="$optionsSqlldr, skip=1" ;;
			-b) base="$2" ; shift ;;
			-s) sep="$2" ; case "$sep" in \\[t]|\\[0-9][0-9][0-9]) sep="`printf "$sep"`" ;; esac ; shift ;;
			-rs) rs="$2" ; shift ;;
			*)
				case "$params" in "") break ;; esac # Plus de param à renseigner? C'est qu'on arrive à la première colonne.
				miamParam "$1" $params
				;;
		esac
		shift
	done
	if [ -z "$*" -o ! -f "$csv" ]
	then
		echo "# Utilisation: oraCopy [-1] [-s <sép>] [-rs <fin de ligne>] <csv> [-b <base>] <table> <colonne>+" >&2
		return 1
	fi
	
	local cols="`for p in "$@" ; do echo "$p" ; done | sed -e 's/:/@/' -e "s/@timestamp/ date 'YYYY-MM-DD HH24:MI:SS'/" -e 's/@char(/ char(/' -e 's/@.*//' | tr '\012' , | sed -e 's/,$//'`"
	
	oraParams "$base" complexe || return 1
	
	{
		# À FAIRE: peut-on ne pas préciser pas ($cols)?
		# Mode "stream" pour avoir des entrées contenant du retour à la ligne:
		#   https://forums.oracle.com/ords/apexds/post/how-can-we-load-data-into-clob-datatype-column-using-sql-lo-1538
		case "$rs" in ?*) rs=" \"str X'`printf "$rs" | od -t x1 | sed -e 's/^[^ ]* *//' -e 's/ //g'`'\"" ;; esac
		cat <<TERMINE
options ($optionsSqlldr)
load data
characterset UTF8
infile '/tmp/`basename "$csv"`'$rs
badfile "$fc.bad"
append into table $table
fields terminated by '$sep'
trailing nullcols
($cols)
TERMINE
	} > $fc.ctl
	
	# NOTE: guillemetage de $BDD_CHAINE
	# Voir https://stackoverflow.com/questions/7409503/is-it-possible-for-oracle-sqlldr-to-accept-a-tns-entry-as-an-instance-qualifier
	# L'autre option, moins maniable, est de faire du userid="$BDD_IM@$BDD_NOM", mais il faut avoir déclaré $BDD_NOM dans le tnsnames.ora et va-t'en le retrouver et le modifier, celui-là.
	
	case "$BDD_SSH" in
		""|localhost)
			[ -z "$BDD_ENV" ] || eval "$BDD_ENV"
			sqlldr userid="\"$BDD_CHAINE\"" $paramsSqlldr data="$csv" < /dev/null
			;;
		*)
			scp -C $fc.ctl "$csv" $BDD_SSH:/tmp/
			ssh $BDD_SSH "$BDD_ENV ; sqlldr userid=\"\\\"$BDD_CHAINE\\\"\" $paramsSqlldr && rm -f $fc.ctl $fc.bad $fc.log $csvd" < /dev/null
			;;
	esac >&2
	
	# À FAIRE: en cas de sortie en erreur récupérer le .log
}

oracle_csvVersTable()
{
	local sep=\; sepl="\012"
	while [ $# -gt 0 ]
	do
		case "$1" in
			-s) sep="$2" ; shift ;;
			-l) sepl="$2" ; shift ;;
			*) break ;;
		esac
		shift
	done
	local table="$1" descr="$2" csv="$3"
	
	oraCopy -s "$sep" -rs "$sepl" -b "$BDD" "$csv" "$table" `awk '/ timestamp(,|$)/{print$1":timestamp";next}/varchar\(([4-9][0-9][0-9][0-9]|[1-9][0-9][0-9][0-9][0-9]+)|text|clob/{print$1":char(999999999)";next}{print$1}' < "$descr"`
	# À FAIRE: exploiter le -rs comme délimiteur d'entrée; permettra de passer des données comportant des retours à la ligne.
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
			-e 's/"[^"]*(INTEGER|FLOAT|TIMESTAMP|TIMESTAMPTZ|DATE).*[^,](,*)$/ <\1>\2/' \
			-e 's/".*[^,](,*)$/ <text>\1/' \
			-e 's/:([^<]*) <[^>]*>/ <\1>/' \
			-e 's/> <[^>]*//' \
			-e 's/([^ ])</\1 </' \
			-e 's/[<>]//g'
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
		awk '{print NR" "$0}' < $T.t | sort -nr > $T.tr
		awk '
			BEGIN{ posd = 0; posf = -1; }
			posd==posf{ next; }
			/^= /{
				# Répartition en files rapides et files lentes.
				# Il est important de lancer dès le départ les lentes (pour ne pas terminer par les requêtes lentes),
				# mais aussi quelques rapides (pour donner l impression d avancer, et remonter les résultats "faciles" au plus vite).
				if(!traiteurs[$2])
				{
					if(ntraiteurs) ++ntraiteurs; else ntraiteurs = 1;
					traiteurs[$2] = (ntraiteurs % 4) == 1 ? -1 : 1; # Un traiteur sur quatre va travailler en sens inverse (commencer par les plus rapides).
				}
				# Au boulot: on lui fournit la prochaine donnée à manger.
				traiteur = $2;
				if(traiteurs[traiteur] < 0)
				{
					if((getline < "'"$T.tr"'") > 0)
					{
						posf = $1;
						for(i=0; ++i < NF;)
							$i=$(i+1);
						--NF;
					}
					else
						posf = posd;
				}
				else
				{
					if((getline < "'"$T.t"'") > 0)
						++posd;
					else
						posd = posf;
				}
				if(posd != posf)
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
		rm $T.t $T.tr
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
