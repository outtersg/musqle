--------------------------------------------------------------------------------
-- NOTE: Tentative de définition d'un agrégat
-- On essaie de définir une fonction d'agrégat sur dates, renvoyant:
-- - la date de départ sur une plage de temps "ponctuelle" (moins de 7 jours)
-- - en outre si cette minimale est un jour rond, et qu'il existe un second minima, daté du même jour mais cette fois avec heure, c'est ce dernier que l'on prend
-- Ainsi sur un ensemble:
-- - de 1978-10-16 à 1979-10-10: l'agrégat renvoie null (plage trop étendue).
-- - de 1979-10-10 à 1979-10-16: 1979-10-10 est renvoyé (ensemble considéré ponctuel, car de moins d'une semaine)
-- - de 1979-10-10 à 1979-10-16, avec une des dates de l'ensemble 1979-10-10 12:34:56: c'est cette dernière qui est prise comme point de départ (on considère que 1979-10-10 00:00:00 a été généré par troncature de notre 12:34:56, donc est une donnée indirecte avec perte d'information)
-- NOTE: performances
-- On compare le résultat de cette chose à une implémentation "bête": à coups de min() et max().
-- Sans appel: la fonction bête tourne en 1,1 s, contre 4,7 s pour la solution ciselée à grand-peine.
-- Deux causes probables:
-- - Les min et max sont implémentés en C
-- - PostgreSQL optimise pour ne pas recalculer deux fois le min quand l'expression comporte deux min(d)
--------------------------------------------------------------------------------

#format delim \t

--------------------------------------------------------------------------------
-- Définition

create or replace function min_date_floue_accu(accu timestamp[], val timestamp)
returns timestamp[]
language plpgsql
strict -- Pour ne pas être appelés sur du null.
immutable
as
$$
	begin
		if accu[1] is null then
			accu[1] = val;
		else
			if val < accu[1] then
				if accu[2] is null or accu[2] < accu[1] then
					accu[2] = accu[1];
				end if;
				accu[1] = val;
			else
				if accu[2] is null or val < accu[2] then
					accu[2] = val;
				end if;
			end if;
		end if;
		
		if accu[3] is null then
			accu[3] = val;
		else
			if val > accu[3] then
				accu[3] = val;
			end if;
		end if;
		
		return accu;
	end;
$$;

create or replace function min_date_floue_restit(accu timestamp[])
returns timestamp
language plpgsql
as
$$
	begin
		return
			case
				when accu[1] is null then null
				when accu[1] < accu[3] - interval '7d' then null
				when accu[1] = date_trunc('d', accu[2]) then accu[2]
				else accu[1]
			end;
	end;
$$;

#if `select count(1) from pg_aggregate a join pg_proc p on p.oid = aggfnoid join pg_type t on t.oid = aggtranstype where proname = 'min_date_floue' and typname = '_timestamp'` >= 1
drop aggregate min_date_floue(timestamp);
#endif

create aggregate min_date_floue(timestamp)
(
	sfunc = min_date_floue_accu,
	finalfunc = min_date_floue_restit,
	stype = timestamp[],
	initcond = '{null,null,null}'
);

--------------------------------------------------------------------------------
-- Test

create temporary table t (id bigserial, d timestamp(0) without time zone);

insert into t (id, d)
	select
		i,
		case
			when i % 12 = 0 or i % 3 = 1 then '1979-10-10 12:34:56'::timestamp + ((i / 3)||'d')::interval
			when i % 12 = 3 then              '1979-10-10'::timestamp + ((i / 3)||'d')::interval
			when i % 12 in (6, 9) then        '1979-10-10 12:34:56'::timestamp + ((i / 3 + 5)||'d')::interval
			when i % 12 = 8 then              '1979-10-10 12:34:56'::timestamp + ((i / 3 + 1)||'d')::interval
			when i % 12 = 11 then             '1979-10-10 12:34:56'::timestamp + ((i / 3 + 10)||'d')::interval
			else null
		end
	from generate_series(0, 999999) s(i);

#define EQU \
	case \
		when min(d) is null or max(d) = min(d) then min(d) \
		when max(d) > min(d) + interval '7d' then null \
		when min(d)::date = min(d) then \
			case \
				when min(case when d <> d::date then d end) < min(d) + interval '1d' then min(case when d <> d::date then d end) \
				else min(d) \
			end \
		else min(d) \
	end

-- Comparaison fonctionnelle

select
	min_date_floue(d) manu,
	EQU minu,
	array_agg(d order by d)
from t
where id < 20
group by id / 3;

-- Test de perfs

#for EXPR in "min_date_floue(d)" EQU
select ''||$$[37mEXPR:$$||'[0m';
#set AVANT `select clock_timestamp()`
with perf as
(
	select EXPR mi
	from t
	group by id / 3
)
select count(1), max(mi) from perf;
select clock_timestamp() - 'AVANT';
#done
