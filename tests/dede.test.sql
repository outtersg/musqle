#include ../dede.pg.sql

create temporary table t(id serial primary key, num varchar(15), truc text, quand timestamp default now(), nombre integer);
insert into t (num, truc, nombre) values ('345', 'coucou', 1);
insert into t (num, truc, nombre) values ('345', 'bouh', 123);
insert into t (num, truc, nombre) values ('999', 'bah', 1);
insert into t (num, truc, nombre) values ('999', 'bah', 123);
-- Distingue-t-on bien un null d'un ''?
insert into t (num, truc, nombre) values ('1000', null, 1);
insert into t (num, truc, nombre) values ('1000', '', 1);
insert into t (num, truc, nombre) values ('99', 'ah', 1);
insert into t (num, truc, nombre) values ('99', 'ah', 1);

create temporary table dep(id serial, tid integer references t(id), m text);
insert into dep (tid, m) values (7, 'fils de 7');
insert into dep (tid, m) values (8, 'fils de 8');

begin;
declare ah cursor for select a.*, b.* from t a join t b on a.num = b.num and b.id > a.id;
select * from diff('ah', '{quand}');

select * from dede_init('t');
select * from t;
select * from dede('t', 1, 2, '{quand}'); -- Celle-ci doit bloquer (différences non ignorables entre les deux entrées).
select * from dede('t', 7, 8, '{quand}'); -- Celle-ci doit passer.
select * from t;
select * from dep;
rollback;

