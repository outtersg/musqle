-- Avec un PHPUnit 6:
-- bdd="pgsql:dbname=test" phpunit ../sqleur/SqleurTestSuite.php tests/detroussages.toutou.test.sql

create temporary table dede_deroule (q timestamp, t text, ref bigint, doublon bigint, err boolean, m text);
#define DETROU_DEROULE dede_deroule
create temporary table t_ignore
(
	s text,
	t text,
	c text
);
#define DETROU_COLONNES_IGNOREES t_ignore

#include ../detroussages.pg.sql

create temporary table t(id bigserial primary key, num varchar(15), truc text, nombre integer, quand timestamp);
insert into t (num, truc, nombre) values ('1 partout', 'coucou', null);
insert into t (num, truc, nombre) values ('1 partout', null, 123);
insert into t (num, truc, nombre) values ('2 sur 0', null, null);
insert into t (num, truc, nombre) values ('2 sur 0', 'bah', 123);
insert into t (num, truc, nombre) values ('1 sur 0', null, 1);
insert into t (num, truc, nombre) values ('1 sur 0', '', 1);
-- Dans le groupe 3, la date de bh doit être complétée, mais pas son nombre, car ah et ch ne sont pas d'accord sur la valeur à mettre (1 ou 2).
insert into t (num, truc, nombre, quand) values ('3', 'ah', 1, '1979-10-10 08:30:00');
insert into t (num, truc, nombre, quand) values ('3', 'bh', null, null);
insert into t (num, truc, nombre, quand) values ('3', 'ch', 2, '1979-10-10 08:30:00');
insert into t (num, truc, nombre) values ('seule', 'borf', null);
insert into t (num, truc, nombre) values ('raf val', 'coucou', 1);
insert into t (num, truc, nombre) values ('raf val', 'coucou', 1);
insert into t (num, truc, nombre) values ('raf null', 'coucou', null);
insert into t (num, truc, nombre) values ('raf null', 'coucou', null);
-- Dans le groupe toutounon, la différence bah / coucou empêche la fusion des autres champs pourtant conciliables (nombre et quand) en mode toutourien.
insert into t (num, truc, nombre, quand) values ('toutououi', 'coucou', null, '1979-10-10 08:30:00');
insert into t (num, truc, nombre, quand) values ('toutououi', null, 123, null);
insert into t (num, truc, nombre, quand) values ('toutounon', 'coucou', null, '1979-10-10 08:30:00');
insert into t (num, truc, nombre, quand) values ('toutounon', 'bah', 123, null);

#test
with
	ids as (select string_agg(id::text, ' ') g from t group by num)
select id, comm from detroussages('t', (select array_agg(g) from ids), true) t(t, id, comm) order by id;
$$
1	"détroué: nombre"
2	"détroué: truc"
3	"détroué: truc nombre"
5	"détroué: truc"
8	"détrouable: quand"
15	"détroué: nombre"
16	"détroué: truc quand"
17	"détrouable: nombre"
18	"détrouable: quand"
$$;

#test
select * from t order by id;
$$
1	"1 partout"	coucou	123	-
2	"1 partout"	coucou	123	-
3	"2 sur 0"	bah	123	-
4	"2 sur 0"	bah	123	-
5	"1 sur 0"	""	1	-
6	"1 sur 0"	""	1	-
7	3	ah	1	"1979-10-10 08:30:00"
8	3	bh	-	-
9	3	ch	2	"1979-10-10 08:30:00"
10	seule	borf	-	-
11	"raf val"	coucou	1	-
12	"raf val"	coucou	1	-
13	"raf null"	coucou	-	-
14	"raf null"	coucou	-	-
15	toutououi	coucou	123	"1979-10-10 08:30:00"
16	toutououi	coucou	123	"1979-10-10 08:30:00"
17	toutounon	coucou	-	"1979-10-10 08:30:00"
18	toutounon	bah	123	-
$$;

#test
select ref, doublon, m from dede_deroule order by coalesce(ref, doublon);
$$
1	-	"détroué: nombre"
-	2	"détroué: truc"
3	-	"détroué: truc nombre"
5	-	"détroué: truc"
15	-	"détroué: nombre"
-	16	"détroué: truc quand"
$$;

-- Maintenant sans le TOUTOUrien:

truncate table dede_deroule;

#test
with
	ids as (select string_agg(id::text, ' ') g from t group by num)
select id, comm from detroussages('t', (select array_agg(g) from ids), null) t(t, id, comm) order by id;
$$
8	"détroué: quand"
17	"détroué: nombre"
18	"détroué: quand"
$$;

#test
select * from t order by id;
$$
1	"1 partout"	coucou	123	-
2	"1 partout"	coucou	123	-
3	"2 sur 0"	bah	123	-
4	"2 sur 0"	bah	123	-
5	"1 sur 0"	""	1	-
6	"1 sur 0"	""	1	-
7	3	ah	1	"1979-10-10 08:30:00"
8	3	bh	-	"1979-10-10 08:30:00"
9	3	ch	2	"1979-10-10 08:30:00"
10	seule	borf	-	-
11	"raf val"	coucou	1	-
12	"raf val"	coucou	1	-
13	"raf null"	coucou	-	-
14	"raf null"	coucou	-	-
15	toutououi	coucou	123	"1979-10-10 08:30:00"
16	toutououi	coucou	123	"1979-10-10 08:30:00"
17	toutounon	coucou	123	"1979-10-10 08:30:00"
18	toutounon	bah	123	"1979-10-10 08:30:00"
$$;

#test
select ref, doublon, m from dede_deroule order by coalesce(ref, doublon);
$$
-	8	"détroué: quand"
17	-	"détroué: nombre"
-	18	"détroué: quand"
$$;
