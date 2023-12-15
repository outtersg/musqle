#define RADE_TEMP_TEMP 1
#define RADE_TEMP radetest_temp
#define RADE_DETAIL radetest_detail
#define RADE_STATS radetest_stats
#define RADE_REF radetest_ref

#if :pilote = "oracle"
#include ../util.oracle.sql
#endif

#define MENAGE <<
$$
drop table if exists radetest_temp;
drop table if exists radetest_detail;
drop table if exists radetest_stats;
drop table if exists radetest_ref;
$$;

#if :pilote = "oracle"
#define MENAGE <<
$$
MENAGE
drop sequence if exists radetest_ref_id_seq;
drop sequence if exists radetest_stats_id_seq;
$$;
#endif

MENAGE

#define RADE_INSTALLER
#include ../rade.sql
#undef RADE_INSTALLER

insert into radetest_temp (indicateur, id) values
	('pasbon', 132);
insert into radetest_temp (indicateur, id) values
	('pasbon', 'tjrspasbon');
#if !RADE_TEMP_TEMP
update radetest_temp set producteur = ':SCRIPT_NAME' where producteur is null;
#endif
update radetest_temp set q = to_date('1978-10-16', 'YYYY-MM-DD') where indicateur = 'pasbon';

#include ../rade.sql

update radetest_detail set a = to_date('1979-10-10', 'YYYY-MM-DD') where indicateur_id in (select id from radetest_ref where indicateur = 'pasbon');

insert into radetest_temp (indicateur, id) values
	('pasbon', 'tjrspasbon');
#if !RADE_TEMP_TEMP
update radetest_temp set producteur = ':SCRIPT_NAME' where producteur is null;
#endif

#include ../rade.sql

select * from radetest_detail order by de, a;

MENAGE
