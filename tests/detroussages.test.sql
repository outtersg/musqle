create temporary table t_ignore
(
	s text,
	t text,
	c text
);
#define DETROU_COLONNES_IGNOREES t_ignore
-- On peut tester la bonne ignorance d'une colonne:
--insert into t_ignore (t, c) values ('t', 'nombre');

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

#test
with
	ids as (select string_agg(id::text, ' ') g from t group by num)
select * from detroussages('t', (select array_agg(g) from ids), null);
$$
1	1	"détroué: nombre"
1	2	"détroué: truc"
5	3	"détroué: truc nombre"
3	5	"détroué: truc"
4	8	"détroué: quand"
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
$$;
