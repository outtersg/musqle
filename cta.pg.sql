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

-- cta: CrossTable Automatique
-- Équivalent à crosstable(), qui détecte les colonnes potentielles.
-- S'invoque en deux fois:
--   select cta($$ select ligne, colonne, valeur from source_données $$);
--   select * from t_cta; -- Table temporaire créée pour l'occasion, dont les colonnes ont le nom et le type voulus.

create or replace function cta(sql text) returns void language plpgsql as
$cta$
	declare
		constit text;
		cols text[];
	begin
		drop table if exists t_cta_d; -- CrossTab Auto Données.
		drop table if exists t_cta;
		-- À FAIRE: permettre de préciser le nom de la table temporaire que l'on veut.
		-- À FAIRE: garder le nom de première colonne.
		execute format('create temporary table t_cta_d as select * from (%s) t(l, c, v)', sql);
		alter table t_cta_d alter column c type text; -- Pour les colonnes numériques ou date, par exemple.
		create index on t_cta_d(l);
		create index on t_cta_d(c);
		with
			col0 as (select c, row_number() over() num from t_cta_d),
			col1 as (select c, min(num) pos from col0 group by 1), -- Les colonnes seront rangées par ordre d'apparition dans les données de départ.
			colt as
			(
				select pos, $$, min(case when c = '$$||replace(c, '''', '''''')||$$' then v end) "$$||c||$$"$$ calcol
				from col1 order by pos
			),
			cols as
			(
				select string_agg(calcol, E'\n' order by pos) calcols from colt
			)
		select
			$$
				create temporary table t_cta as
					select
						l
						$$||replace(calcols, E'\n', E'\n\t\t\t\t\t\t')||$$
					from t_cta_d
					group by 1
			$$
		into constit
		from cols;
		execute constit;
	end;
$cta$;
