-- Copyright (c) 2020-2021 Guillaume Outters
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

#define for_COLONNE_in_cols \
	$$||(select string_agg(replace(replace($$
#define done_COLONNE_in_cols \
	$$, 'COLONNE_TRADUITE', coalesce(tt.v, '_source.COLONNE')), 'COLONNE', t.col), E'\n') \
	from unnest(cols) t(col) \
	left join unnest(colsTraduites) tt(c, v) on tt.c = t.col \
	left join unnest(agregats) ta(col, agreg) on ta.col = t.col \
	left join unnest(agressifs) tag(col, agressif) on tag.col = t.col \
	left join unnest(nulls) tn(col, equivalent_null) on tn.col = t.col \
	)||$$
#define array_agg_cols(CONDITION) \
	regexp_replace \
	( \
		'{' \
			for_COLONNE_in_cols \
				||case when CONDITION then ',COLONNE' else '' end \
			done_COLONNE_in_cols \
		||'}', \
		'^{,', \
		'{' \
	)::text[]

with
	taches as
	(
		select row_number() over() tache, string_to_array(g, ' ')::bigint[] ids from unnest(groupes) t(g)
	),
	-- Sur quels champs les entrées possédant de la donnée se mettent-elles d'accord?
	daccord0 as
	(
		select
			tache, ids
			-- NOTE: fonction agrégée
			-- Ici il a été tenté de reposer sur une seule fonction d'agrégat:
			--   , sansdistinctionaucune(COLONNE_TRADUITE) COLONNE
			-- supposée + rapide (une seule au lieu des 2 passes min et max) et + efficace (dès la première valeur ≠ on peut sortir (en tt cas marquer l'accu ĉ noop).
			-- En pratique + lent, car min et max ultra optimisées, cf. tests/agg.date.sql
			for_COLONNE_in_cols
			, $$||coalesce
			(
				agressif,
				$$ case when max(COLONNE_TRADUITE) $$||case when agreg is not null then 'is not distinct from' else '=' end||$$ min(COLONNE_TRADUITE) then max(COLONNE_TRADUITE)$$||coalesce(' else '||agreg, '')||$$ end $$
			)||$$ as COLONNE
			, count(COLONNE_TRADUITE) COLONNE__nv__ -- Nombre de valeurs non null sur cette colonne.
			done_COLONNE_in_cols
		from taches join $$||nomTable||$$ _source on _source.id = any(taches.ids)
		group by 1, 2
		having count(1) > 1 -- Inutile de comparer une entrée toute seule avec elle-même.
	),
	daccord as
	(
		-- À FAIRE: permettre à null d'être une valeur comme une autre (agrégeable etc.).
		-- En ce cas l'agrég devrait renvoyer non pas la valeur d'agrégat, mais un (val, alignee bool).
		-- Cependant cela pose des difficultés techniques car il faudrait alors créer un type composite par type de colonne.
		-- Pour le moment ce succès d'alignement est calculé à partir des null (si alignement = null alors qu'il y a au moins une valeur non nulle, c'est qu'on n'a pas trouvé d'accord sur ce champ).
		select
			tache, ids,
			for_COLONNE_in_cols
			COLONNE,
			done_COLONNE_in_cols
			array_agg_cols(COLONNE__nv__ > 0 and COLONNE is null) nons
		from daccord0
	),
	maj as
	(
		select
			daccord.tache,
			_source.id,
			array_agg_cols(daccord.COLONNE is not null and (COLONNE_TRADUITE is null or COLONNE_TRADUITE <> daccord.COLONNE)) ouis
		from daccord join $$||nomTable||$$ _source on _source.id = any(ids)
	),
	afaire as
	(
		select
			maj.tache, _source.id,
			for_COLONNE_in_cols
			case when 'COLONNE' = any(ouis) then daccord.COLONNE else COLONNE_TRADUITE end as COLONNE,
			done_COLONNE_in_cols
			ouis
		from maj
		join daccord using(tache)
		join $$||nomTable||$$ _source using(id)
		-- Ne sélectionnons l'entrée que si au moins un de ses champs va être modifié.
		-- Ça évite d'encrasser avec une table temporaire comportant toutes les colonnes pour rien.
		where array_length(ouis, 1) > 0
		$$||case when toutou then $$
		and array_length(nons, 1) is null -- Eh oui, un tableau vide a une longueur nulle et non 0!
		$$ else '' end||$$
	),
#if defined(DETROU_HISTO_COMM)
#if 0
	$$||case when exists(select 1 from pg_tables where nomTable||'DETROU_CIMETIERE' in (schemaname||'.'||tablename, tablename)) then $$
#endif
	histo as
	(
			with ids as (select id, ids from afaire join daccord using(tache))
		-- Le count() nous garantit un seul résultat, ce qui nous assure de pouvoir ensuite faire une jointure sur le résultat sans risquer d'exploser la cardinalité.
		select count(ohoh('$$||nomTable||$$', id, null, DETROU_HISTO_COMM)) n from ids
	),
#if 0
	$$ else '' end||$$
#endif
#endif
	maj0 as
	(
		update $$||nomTable||$$ _source
		set
			-- À FAIRE: sur l'equivalent_null, prendre null ou son équivalent selon la valeur la plus fréquemment observée dans le jeu d'entrées.
			id = _source.id -- Histoire de pouvoir commencer par une virgule ensuite.
			for_COLONNE_in_cols
			, COLONNE = $$||coalesce($$case when afaire.COLONNE = '$$||replace(equivalent_null, '''', $$''$$)||$$' then null else afaire.COLONNE end$$, 'afaire.COLONNE')||$$
			done_COLONNE_in_cols
		from afaire
#if defined(DETROU_HISTO_COMM)
		-- Jointure avec histo pour être sûrs qu'elle est appelée (si histo utilisait un update ou un insert PostgreSQL l'appellerait systématiquement, mais comme il a une forme de select il nous faut forcer la jointure).
		join histo on true
#endif
		where _source.id = afaire.id
		returning afaire.tache, _source.id, afaire.ouis
#if defined(DETROU_DEROULE)
		, clock_timestamp() q
	),
	majj as
	(
		insert into DETROU_DEROULE
			select
				maj0.q,
				'$$||nomTable||$$',
				case when maj0.id = taches.ids[1] then maj0.id else null end,
				case when maj0.id <> taches.ids[1] then maj0.id else null end,
				false,
				'détroué: '||array_to_string(ouis, ' ') info
			from taches join maj0 using(tache)
		returning coalesce(ref, doublon)
#endif
	)
