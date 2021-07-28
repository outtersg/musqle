-- Avec un PHPUnit 6:
-- bdd="pgsql:dbname=test" phpunit ../sqleur/SqleurTestSuite.php tests/detroussages.agregat.test.sql

create temporary table t_cols
(
	s text,
	t text,
	c text,
	options text
);
#define DETROU_COLONNES_IGNOREES t_cols
#define DETROU_COLONNES_IGNOREES_FILTRE and options is null
#define DETROU_AGREG 1

#include ../detroussages.pg.sql

create temporary table t(id bigserial primary key, num varchar(15), truc text, nombre integer, quand timestamp);
insert into t (num, truc, nombre, quand) values ('mouais', 'ah', 1, '1979-10-10 08:30:00');
insert into t (num, truc, nombre, quand) values ('mouais', 'bh', null, null);
insert into t (num, truc, nombre, quand) values ('mouais', 'ch', 2, '1979-10-16 10:30:00');
insert into t (num, truc, nombre, quand) values ('pasposs', 'ah', 1, '1979-10-10 08:30:00');
insert into t (num, truc, nombre, quand) values ('pasposs', 'bh', null, null);
insert into t (num, truc, nombre, quand) values ('pasposs', 'ch', 2, '1978-10-16 10:30:00');

with
	ids as (select string_agg(id::text, ' ') g from t group by num)
select * from detroussages('t', (select array_agg(g) from ids), null) order by id;

#test
select * from t order by id;
$$
1	mouais	ah	1	"1979-10-10 08:30:00"
2	mouais	bh	-	-
3	mouais	ch	2	"1979-10-16 10:30:00"
4	pasposs	ah	1	"1979-10-10 08:30:00"
5	pasposs	bh	-	-
6	pasposs	ch	2	"1978-10-16 10:30:00"
$$;

-- Insertion d'une règle autorisant la fusion de 'quand' lorsque les différentes entrées ont moins d'une semaine d'écart.
insert into t_cols (t, c, options) values ('t', 'quand', $$ DETROU_AGREG: DETROU_AGREG_DATE_FLOUE_MAX(7) $$);
-- La fonction interne de détroussage doit être recalculée pour cette session.
select set_config('detrou._detroussages_fonc_t', null, false);
-- Quelques entrées supplémentaires pour s'assurer que ça ne casse pas le comportement habituel (si un null et une valeur, celle-ci est reprise partout).
insert into t (num, truc, nombre, quand) values ('1', 'bh', null, null);
insert into t (num, truc, nombre, quand) values ('1', 'bh', null, null);
insert into t (num, truc, nombre, quand) values ('2', 'bh', null, '1979-10-10 08:30:00');
insert into t (num, truc, nombre, quand) values ('2', 'bh', null, null);
with
	ids as (select string_agg(id::text, ' ') g from t group by num)
select * from detroussages('t', (select array_agg(g) from ids), null) order by id;

#test
select * from t order by id;
$$
1	mouais	ah	1	"1979-10-16 10:30:00"
2	mouais	bh	-	"1979-10-16 10:30:00"
3	mouais	ch	2	"1979-10-16 10:30:00"
4	pasposs	ah	1	"1979-10-10 08:30:00"
5	pasposs	bh	-	-
6	pasposs	ch	2	"1978-10-16 10:30:00"
7	1	bh	-	-
8	1	bh	-	-
9	2	bh	-	"1979-10-10 08:30:00"
10	2	bh	-	"1979-10-10 08:30:00"
$$;
