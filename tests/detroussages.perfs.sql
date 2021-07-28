-- NOTE: perfs avec cache de la fonction générée sous-jacente
-- Sur une table de 4 colonnes, on teste les deux façons:
-- - sans cache (fonction recréée à chaque appel, cf. bloc #if 0 plus bas à caler en tant que corps de detrou()).
-- - avec cache (fonction créée une seule fois par session (pour une table donnée)).
-- 2500 appels: 9,6 s / 4,6 s
-- 5000 appels: 29 s / 14 s
-- Bref sur une table de 4 colonnes, la constitution de la fonction bouffe autant de temps que son exécution.

-- bdd="pgsql:dbname=test" php ../sqleur/sql2csv.php tests/detroussages.perfs.sql

#if 0
drop function if exists _detroussages_fonc(text[]);
execute detroussages_fonc_table('_detroussages_fonc', nomTable, toutou);
return query select * from _detroussages_fonc(groupes);
#endif

create temporary table t_ignore
(
	s text,
	t text,
	c text
);
#define DETROU_COLONNES_IGNOREES t_ignore

#include ../detroussages.pg.sql

create temporary table t(id bigserial primary key, num varchar(31), truc text, nombre integer, quand timestamp);
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

-- Multiplication des données pour tester les perfs.
insert into t (num, truc, nombre, quand)
	select n||'.'||num, truc, nombre, quand
	from generate_series(1, 500) n(n), t; -- 500 -> 2500 appels car nous avons 10 entrées regroupées par paquets de 2 (en moyenne), soit 5 appels.

#set AVANT `select clock_timestamp()`
with
	ids as (select string_agg(id::text, ' ') g from t group by num),
	d as (select * from ids, detroussages('t', array[g], null))
select count(*) from d;
select clock_timestamp() - 'AVANT';

-- Notez que l'on n'a pas fait:
--   select * from detroussages('t', (select array_agg(g) from ids), null)
-- qui serait nettement plus efficace (un seul appel à detroussages, capable de traiter 200000 appels en 10 s),
-- mais inintéressant pour notre mesure de perfs,
-- en outre non réaliste (généralement les gens détrouent couple par couple, et non pas établissent la liste de tous les couples pour ensuite les passer en une seule fois).
