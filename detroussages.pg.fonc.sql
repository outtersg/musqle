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
	$$||(select string_agg(replace($$
#define done_COLONNE_in_cols \
	$$, 'COLONNE', col), E'\n') from unnest(cols) t(col))||$$

with
	taches as
	(
		select row_number() over() tache, string_to_array(g, ' ')::bigint[] ids from unnest(groupes) t(g)
	),
	mamie as
	(
		select
			tache, ids
			for_COLONNE_in_cols
			, max(_source.COLONNE) COLONNE_max, min(_source.COLONNE) COLONNE_min
			done_COLONNE_in_cols
		from taches join $$||nomTable||$$ _source on _source.id = any(taches.ids)
		group by 1, 2
		having count(1) > 1 -- Inutile de comparer une entrée toute seule avec elle-même.
	),
	-- Sur quels champs les entrées possédant de la donnée se mettent-elles d'accord?
	daccord as
	(
		select
			tache, ids
			for_COLONNE_in_cols
			, case when COLONNE_max = COLONNE_min then COLONNE_max end COLONNE
			done_COLONNE_in_cols
		from mamie
	),
	afaire as
	(
		select
			daccord.tache, _source.id
			for_COLONNE_in_cols
			, coalesce(_source.COLONNE, daccord.COLONNE) COLONNE
			done_COLONNE_in_cols
			, ''
			for_COLONNE_in_cols
			||case when _source.COLONNE is null and daccord.COLONNE is not null then ' COLONNE' else '' end
			done_COLONNE_in_cols
			_modifs
		from daccord join $$||nomTable||$$ _source on _source.id = any(ids)
		where false
		-- Ne sélectionnons l'entrée que si au moins un de ses champs va être modifié.
		for_COLONNE_in_cols
		or (_source.COLONNE is null and daccord.COLONNE is not null)
		done_COLONNE_in_cols
	),
#if defined(DETROU_DEROULE)
	maj0 as
#else
	maj as
#endif
	(
		update $$||nomTable||$$ _source
		set
			id = _source.id -- Histoire de pouvoir commencer par une virgule ensuite.
			for_COLONNE_in_cols
			, COLONNE = afaire.COLONNE
			done_COLONNE_in_cols
		from afaire
		where _source.id = afaire.id
		returning afaire.tache, _source.id, 'détroué:'||_modifs info
#if defined(DETROU_DEROULE)
		, clock_timestamp() q
	),
	maj as
	(
		select tache, id, info from maj0
	),
	majj as
	(
		insert into DETROU_DEROULE
			select
				maj0.q,
				'''$$||nomTable||$$''',
				case when maj0.id = taches.ids[1] then maj0.id else null end,
				case when maj0.id <> taches.ids[1] then maj0.id else null end,
				false,
				maj0.info
			from taches join maj0 on taches.tache = maj0.tache
		returning coalesce(ref, doublon)
#endif
	)
