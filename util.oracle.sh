# À inclure depuis votre .bashrc ou autre.
# Pensez à définir $BDD_NOM, $BDD_QUI, $BDD_MDP.

BDD_SQLEUR=sqloracle

sqloracle()
{
	[ -n "$BDD_MDP" ] || { echo "# Veuillez définir la variable \$BDD_MDP." >&2 ; return 1 ; }
	[ -n "$BDD_HOTE" ] || BDD_HOTE=localhost
	[ -n "$BDD_PORT" ] || BDD_PORT=1521
	
	sqlplus -s $BDD_QUI/$BDD_MDP@"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=$BDD_HOTE)(PORT=$BDD_PORT))(CONNECT_DATA=(SID=$BDD_NOM)))"
}

# Dans quelle table figure tel RowID?
# https://chartio.com/resources/tutorials/how-to-list-all-tables-in-oracle/
quelletable()
{
	T=/tmp/temp.qt.$$
	
	STATS="and num_rows > 0"
	
	GREP="stdbuf -oL grep"
	EGREP="stdbuf -oL egrep"
	SED="stdbuf -oL sed"
	
	groui() { $EGREP '^oui|^\?' ; }
	grouin() { $GREP '^[?1-9]' ; }
	QUOI="'oui', row_id"
	GROUI=groui
	
	c=row_id
	
	while [ $# -gt 0 ]
	do
		case "$1" in
			# -a: récupère aussi les tables sans statistiques.
			-a) STATS="and (num_rows is null or num_rows > 0)" ;;
			-n) QUOI="count(1)" ; GROUI=grouin ;;
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
	
	nFaits=-4
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
				detailProgression="`incruster "$progression" "$detailProgression" -b 0 / 5`"
				detailProgression="`incruster -c "$info" "$detailProgression" -b $colonne / 5`"
				progression="$detailProgression"
			fi
			chrono $colonne "$info"
		fi
		printf "\\r%s" "$progression"
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
	} | $BDD_SQLEUR | awk '{f="'"$T"'.t."(NR%4);print>f}'
	
	nAFaire=`cat $T.t.? | wc -l`
	printf "Recherche parmi %d colonnes de %d tables\n" "$nAFaire" "`cat $T.t.? | cut -d ' ' -f 1 | sort -u | wc -l`" >&2
	{
		i=0
	for f in $T.t.[0123]
	do
		{
			echo "set pagesize 0;"
			while read t c
			do
				echo "select '? $i $t.$c' from dual;"
				echo "select $QUOI, '$t.$c' from $t where $c in ('$trucs');"
			done < $f
			} | { $BDD_SQLEUR 2>&1 ; echo "? $i (FINI)" ; } | $SED -e 's/^  *//' | $GROUI &
		i=`expr $i + 1`
	done
	wait
	} | while read l
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
