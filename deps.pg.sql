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

-- Dépendances inter-tables.

-- À FAIRE: pour les décomptes, préciser entre crochets le critère qui a permis de trouver les entrées (ex.: deps('personnes', 45) doit renvoyer "enfants[parent=45]: 7" pour signifier que 7 personnes ont pour parent la personne 45).
-- À FAIRE: indentation
-- À FAIRE: se protéger des boucles infinies (cumul).

drop type if exists deps_ids cascade;
create type deps_ids as (t text, ids bigint[]);

create or replace function deps(nomTable text, _id bigint) returns table(t text, id bigint) language plpgsql as
$$
	declare
		cumul deps_ids[];
		àFaire deps_ids[];
	begin
		-- Du fait de l'impossibilité de faire de l'agrégation dans des CTE, mais de notre besoin de le faire (pour interroger chaque table par lot plutôt qu'unitairement), on abuse de fonctions et de tableaux émulant du recursive.
		select array[(nomTable, array[_id])] into àFaire;
		while array_length(àFaire, 1) > 0 loop
			with
				af as (select * from unnest(àFaire) af where af.t not like '%]'),
				d_ as (select dede_dependances(af.t) d from af),
				d as (select (d).vs||'.'||(d).vt vt, (d).vc, (d).ds||'.'||(d).dt dt, (d).dc from d_),
				-- On distingue les tables portant un ID (dont on renverra la colonne id) des autres (dont on renverra un simple décompte).
				aid as
				(
					-- https://dba.stackexchange.com/a/22420
					select
						d.*,
						exists(select 1 from pg_attribute where attrelid = d.dt::regclass and attnum > 0 and not attisdropped and attname = 'id') aid
					from d
				),
				naf as
				(
					select d.dt||case when aid then '' else '[]' end t, deps_exec_ids(d.dt, d.dc, af.ids, d.aid) tid, d.aid
					from af join aid d on af.t = d.vt
				),
				tids as
				(
					select (naf.t, array_agg(tid))::deps_ids tids
					from naf
					where tid <> 0 or aid -- Les 0 ne nous intéressent que s'ils représentent un id, pas un décompte.
					group by naf.t
				)
			select array_agg(tids) into àFaire from tids;
			return query
				with t as (select * from unnest(àFaire))
				select t.t, unnest(t.ids) id from t;
		end loop;
	end;
$$;

comment on function deps(text, bigint) is
$$Trouve récursivement, pour l'entrée _id d'une table nomTable, toutes les entrées dépendantes par clé étrangère.
- Si l'entrée dépendante est elle-même dotée d'un id, elle est à son tour explorée
- Sinon la table dépendante est simplement récapitulée par son nombre d'entrées
Ex.: si la table personne possède deux colonnes pere et mere, alors deps('personne', 1) énumérera tous les enfants de 1 (et ses petits-enfants);
mais si la relation est du 1 à n, par une table intermédiaire de parenté (1 personne pouvant avoir de 0 à une infinité de parents), on signalera simplement le nombre d'enfants de 1.
$$;

create or replace function deps_exec_ids(_t text, _c text, ids bigint[], renvoieIds boolean) returns table(id bigint) language plpgsql as
$$
	begin
		return query execute format('select %s from '||_t||' where '||_c||' = any($1)', case when renvoieIds then 'id::bigint' else 'count(1)::bigint' end) using ids;
	end;
$$;
