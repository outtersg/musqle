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
	# -a: récupère aussi les tables sans statistiques.
	STATS="and num_rows > 0"
	if [ "x$1" = x-a ] ; then STATS="and (num_rows is null or num_rows > 0)" ; shift ; fi
	
	c=row_id
	
	guillemette() { sed -e "s/ /','/g" ; }
	trucs="`echo "$*" | guillemette`"
	
	{
		cat <<TERMINE
set pagesize 0
set feedback off

select t.table_name
from all_tables t, all_tab_columns c
where
	t.owner = '$BDD_QUI' and c.table_name = t.table_name and c.owner = t.owner and lower(column_name) = lower('$c')
	$STATS
order by t.table_name desc;
TERMINE
	} | $BDD_SQLEUR | awk '{f="/tmp/temp.tables."(NR%4);print>f}'
	
	printf "Recherche parmi %d tables\n" "`cat /tmp/temp.tables.? | wc -l`" >&2
	for f in /tmp/temp.tables.[0123]
	do
		{
			echo "set pagesize 0;"
			while read t
			do
				echo "select 'oui', '$t', $c from $t where $c in ('$trucs');"
			done < $f
		} | $BDD_SQLEUR 2>&1 | grep "^oui" &
	done
	wait
}
