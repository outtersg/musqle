-- Copyright (c) 2021 Guillaume Outters
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in
-- all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
-- SOFTWARE.

-- À FAIRE: getenv(LINES)
-- À FAIRE: marquer les ordonnées remarquables, ex. si l'on va de 0.2 à 1.8, marquer 0.25, 0.5, 0.75, 1, 1.25, 1.5, 1.75.
-- À FAIRE: passer la légende en options?
-- À FAIRE: avoir des séries à trous (ne pas forcer toutes les séries à avoir une valeur pour tout x).

create or replace function graphe(nomTable text, colg text, colx text, coly text, options text[]) returns table(y float, graphe text) language plpgsql as
$$
	declare
		d float[][];
		op2 text[];
	begin
		execute format
		(
			$e$
				with
					d as
					(
						select %s g, array_agg(%s order by %s) d
						from %s
						group by 1
						order by 1
					)
				select
					array_agg(d order by g),
					'{}'
				from d
			$e$,
			colg,
			coly,
			colx,
			nomTable
		) into d, op2;
		return query select * from graphe(d, options||op2);
	end;
$$;

create or replace function _graphe_sym(series integer[], options text[]) returns text immutable language sql as
$$
	select
		case
			when series is null then ' '
		else
			case series
				when '{1}' then case when 'ANSI' = any(options) then '[33mx[0m' else 'x' end
				when '{2}' then case when 'ANSI' = any(options) then '[36m+[0m' else '+' end
				when '{1,2}' then '*'
				else array_to_string(series, ',')
			end
		end
	;
$$;

create or replace function graphe(d float[][], options text[]) returns table(y float, graphe text) language sql as
$$
	with
		params as
		(
			with
				op as (select unnest(options) opt)
			select
				case when opt like '%:%' then split_part(opt, ':', 1) else opt end var,
				case when opt like '%:%' then split_part(opt, ':', 2) end val
			from op
		),
		serie as (select generate_series(1, array_length(d, 1)) serie),
		ds as (select serie, d[serie:serie] d from serie),
		points as
		(
			select serie, row_number() over(partition by serie) x, y.y
			from ds, unnest(ds.d) y(y)
		),
		vals as
		(
			select unnest(d) y
		),
		mamie as -- MAx / MIn de l'Ensemble.
		(
			select max(y) maxou, case when '0' = any(options) then 0.0 else min(y) end minou from vals
		),
		dims as
		(
			with
				optionnumerique as (select var::integer from params where var ~ '^[1-9][0-9]*$'),
				ny as (select count(distinct y) ny from vals)
			select max(x) nx, max(coalesce(o.var, ny)) ny
			from points
			join ny on true
			left join optionnumerique o on true
		),
		cases as
		(
			select serie, x, round((y - minou) / (maxou - minou) * ny) y
			from mamie, points, dims
		),
		cg as -- Case groupées.
		(
			select x, y, array_agg(serie order by serie) series
			from cases
			group by x, y
		),
		t as
		(
			select axex.x, axey.y, series
			from dims
			join generate_series(1, dims.nx) axex(x) on true
			join generate_series(1, dims.ny) axey(y) on true
			left join cg using(x, y)
		),
		aff as
		(
			select y, string_agg(_graphe_sym(series, options), '' order by x) ligne
			from t group by 1 order by 1 desc
		)
	select minou + (y - 0.5) * (maxou - minou) / ny , ligne
	from aff, mamie, dims;
$$;
