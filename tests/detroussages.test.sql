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

with
	ids as (select string_agg(id::text, ' ') g from t group by num)
select * from detroussages('t', (select array_agg(g) from ids), null);
select * from t order by id;
