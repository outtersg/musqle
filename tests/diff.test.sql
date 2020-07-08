#include ../diff.sql

create temporary table t(id serial, num varchar(15), truc text, quand timestamp default now(), nombre integer);
insert into t (num, truc, nombre) values ('345', 'coucou', 1);
insert into t (num, truc, nombre) values ('345', 'bouh', 123);
insert into t (num, truc, nombre) values ('999', 'bah', 1);
insert into t (num, truc, nombre) values ('999', 'bah', 123);
-- Distingue-t-on bien un null d'un ''?
insert into t (num, truc, nombre) values ('1000', null, 1);
insert into t (num, truc, nombre) values ('1000', '', 1);

begin;
declare ah cursor for select a.*, b.* from t a join t b on a.num = b.num and b.id > a.id;
select * from diff('ah');
rollback;

