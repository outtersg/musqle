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

#if defined(DEDE_CIMETIERE) and defined(DETROU_CIMETIERE_COLS) and not defined(DETROU_CIMETIERE)
#set DETROU_CIMETIERE DEDE_CIMETIERE
#endif

#if defined(DETROU_CIMETIERE) and not defined(DETROU_CIMETIERE_COLS)
#define DETROU_CIMETIERE_COLS nouveau
#endif

#define for_COLONNE_in_cols \
	$$||(select string_agg(replace(replace($$
#define done_COLONNE_in_cols \
	$$, 'COLONNE_TRADUITE', coalesce(tt.v, '_source.COLONNE')), 'COLONNE', col), E'\n') from unnest(cols) t(col) left join unnest(colsTraduites) tt(c, v) on tt.c = t.col)||$$

with
	taches as
	(
		select row_number() over() tache, string_to_array(g, ' ')::bigint[] ids from unnest(groupes) t(g)
	),
	-- Sur quels champs les entrées possédant de la donnée se mettent-elles d'accord?
	daccord as
	(
		select
			tache, ids
			for_COLONNE_in_cols
			, case when max(COLONNE_TRADUITE) = min(COLONNE_TRADUITE) then max(COLONNE_TRADUITE) end COLONNE
			done_COLONNE_in_cols
		from taches join $$||nomTable||$$ _source on _source.id = any(taches.ids)
		group by 1, 2
		having count(1) > 1 -- Inutile de comparer une entrée toute seule avec elle-même.
	),
	afaire as
	(
		select
			daccord.tache, _source.id
			for_COLONNE_in_cols
			, coalesce(COLONNE_TRADUITE, daccord.COLONNE) COLONNE
			done_COLONNE_in_cols
			, ''
			for_COLONNE_in_cols
			||case when COLONNE_TRADUITE is null and daccord.COLONNE is not null then ' COLONNE' else '' end
			done_COLONNE_in_cols
			_modifs
		from daccord join $$||nomTable||$$ _source on _source.id = any(ids)
		where false
		-- Ne sélectionnons l'entrée que si au moins un de ses champs va être modifié.
		for_COLONNE_in_cols
		or (COLONNE_TRADUITE is null and daccord.COLONNE is not null)
		done_COLONNE_in_cols
	),
#if defined(DETROU_CIMETIERE)
	$$||case when exists(select 1 from pg_tables where nomTable||'DETROU_CIMETIERE' in (schemaname||'.'||tablename, tablename)) then $$
	histo as
	(
		insert into $$||nomTable||$$DETROU_CIMETIERE
			with ids as (select id, ids from afaire join daccord using(tache))
			select DETROU_CIMETIERE_COLS, t.*
			from ids join $$||nomTable||$$ t using(id)
		returning id
	),
	$$ else '' end||$$
#endif
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
				'$$||nomTable||$$',
				case when maj0.id = taches.ids[1] then maj0.id else null end,
				case when maj0.id <> taches.ids[1] then maj0.id else null end,
				false,
				maj0.info
			from taches join maj0 on taches.tache = maj0.tache
		returning coalesce(ref, doublon)
#endif
	)
