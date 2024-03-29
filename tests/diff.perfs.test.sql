#include ../diff.sql

#define NENTREES 20000

create temporary table t(id serial, num varchar(15), truc text, quand timestamp default now(), nombre integer);
create temporary table chrono (variante text, t float);

-- Pas de hasard, on souhaite du reproductible.
insert into t (num, truc, nombre)
	with i as (select i from generate_series(1, NENTREES) t(i))
	select
		case when i % 6 in (0, 1) then 'A1426B7' else 'Z8797214C0' end,
		case when i % 4 in (0, 1) then 'yougloudoughioug' else 'youpi' end,
		(i % 7) / 2
	from i;

#for BOUCLE in 0 1 2 3 4 5 6 7 8 9
#include ../diff.sql
begin;
select count(*) from diff('select a.*, b.* from t a join t b on b.id = a.id + 1');
insert into chrono select 'tout', extract(epoch from clock_timestamp() - now());
commit;
begin;
select count(*) from diff('select a.*, b.* from t a join t b on b.id = a.id + 1', null, '{nombre:0}');
insert into chrono select 'filtré', extract(epoch from clock_timestamp() - now());
commit;
#done

#include ../graphe.sql

select * from graphe('chrono', 'variante', 'variante', 't', '{ANSI,40}');
select variante, avg(t) from chrono group by 1 order by 2 desc;

-- Mesure de perfs 2021-04-04 (pg 9.5)
-- Comparaison croisée sur 10 essais de 20000 entrées chacun (28095 différences déterminées) en faisant varier:
-- - Sélection des colonnes:
--   - A: a.col > 0 and a.c = any(cols)
--   - B: a.col between 1 and ncols - 1
-- - Filtrage des récessifs:
--   - 0: b.v is distinct from a.v and not (a.c = any(recessifs) and a.v is null)
--   - 2: b.v is distinct from a.v puis select from ce premier filtrage where not (res.c = any(recessifs) and res.va is null)
-- - Ignorance des champs:
--   - -: sans le code qui ignore les champs
--   - i: avec le code qui ignore (notre cas de test n'ignore rien mais joue tout de même le code pour)
-- Verdict: moyenne en secondes par passe:
--   1,57 A2i
--   1,53 A0i/B2i
--   1,48 A0-/A2-
--   1,46 B0i
--   1,43 B0-/B2-
-- Constatations (N.B.: on approxime en parlant de % alors qu'il s'agit de "pour cent cinquante"):
-- - A est pénalisé de 4 à 7 % par rapport à B (mais est nécessaire lorsque certaines colonnes sont exclues via 'sauf') => une solution intermédiaire B+A permet de regagner 1 %, voire 2 avec une logique inverse (cf. AB0ibis plus bas).
-- - i prend 3 à 10 % de plus que - (mais c'est normal: plus de travail)
-- - 0 ou 2 sont équivalents pour - (normal: le filtrage supplémentaire n'est appliqué qu'en présence de filtres)
-- - avantage à 0 (4 à 7 %) en cas de filtrage => a priori cette solution (mais on va essayer d'affiner ci-dessous)

-- Mesure de perfs bis:
-- En tentant deux variations:
-- - 1: b.v is distinct from a.v and not (a.c = any(recessifs) and a.v is null) -> case when b.v is not distinct from a.v then false when a.c = any(recessifs) and a.v is null then false else true end [idée: que le case court-circuite le calcul de son second when lorsque le premier renvoie false]
-- - 3: a join b join ids -> a, b, ids
-- On se rend compte que les deux aboutissent au même plan d'exécution que l'original (que l'on conservera pour sa simplicité d'écriture).

-- Mesure de perfs avec l'implémentation complète des recessifs:
-- - coalesce(a.c||':'||a.v, a.c) = any(recessifs) grappille 1 à 2 s sur case when a.v is null then a.c else a.c||':'||a.v end = any(recessifs).
-- - CETTE FOIS LES VARIANTES 1 ET SURTOUT 3 GAGNENT 1 à 2 %
-- Pour cette raison on bascule tout le monde sur l'implémentation 1.

-- Mesure de perfs quater:
-- On reprend les mesures après s'être rendu compte qu'éliminer A était une erreur (B seul ne prend pas en compte le paramètre 'sauf'):
-- (N.B.: par rapport à la première mesure, on a maintenant l'implémentation des recessifs qui tourne)
--   1.55 A0i
--   1.51 AB0i
--   1.49 AB0ibis (en logique inverse and not a.c = any(sauf))
--   1.48 A0- / B0i
--   1.46 AB0-
--   1.44 AB0-bis (avec un case when sauf is null then true else … end pour optimiser le cas où sauf est vide)
--   1.42 B0-
