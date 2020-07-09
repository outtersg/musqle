-- Copyright (c) 2020 Guillaume Outters
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

-- dede: DÉdoublonnage blinDÉ, ou DÉdoublonnage DÉcontracté car la fonction nous garantit qu'on ne casse rien.
-- fulbert:
--         Fusion        (deux données référençant la même réalité)
--         Unilatérale   (on ramène tout à un seul des deux enregistrements)
--         Liens         (en particulier les entrées liées par une clé étrangère)
--       + Bretelles     (mais on historise aussi l'intégralité de l'entrée, en cas de "clé étrangère applicative" (dépendance entre tables non déclarée en base mais sur laquelle repose l'applicatif)
--     des Entrées
--         Redondantes
--   d'une Table

#define CIMETIERE _poubelle

#if defined(DEDE_CLES_ETRANGERES_APPLICATIVES)
#if `select count(*) from pg_tables where tablename = 'DEDE_CLES_ETRANGERES_APPLICATIVES'` = 0
create table DEDE_CLES_ETRANGERES_APPLICATIVES
(
	id serial primary key,
	vs text,
	vt text,
	vc text,
	ds text,
	dt text,
	dc text
);
#endif
#endif

#include diff.pg.sql

drop type if exists dede_champ cascade;
create type dede_champ as ("table" text, champ text, schema text);

create or replace function dede(nomTable text, ancien bigint, nouveau bigint, clesEtrangeresApplicatives dede_champ[], diffSaufSurColonnes text[]) returns table(id bigint, err text) as
$$
	declare
		curdi refcursor;
	begin
		-- Vérification des données.
		
		if diffSaufSurColonnes is not null then
			--return query select * from dede_execre('select * from '||nomTable||'_dede_diff('||ancien||', '||nouveau||execute dedeselect * from dede_diff(nomTable, 
			return query execute 'select * from '||nomTable||'_dede_diff($1, $2, $3)' using ancien, nouveau, diffSaufSurColonnes;
			if found then
				return;
			end if;
		end if;
		
		-- Vérification des clés étrangères.
		
		perform dede_exec('update '||ds||'.'||dt||' set '||dc||' = '||nouveau||' where '||dc||'::bigint = '||ancien)
		from dede_dependances(nomTable, clesEtrangeresApplicatives);
		
		-- Historisation.
		
		perform dede_exec('insert into '||nomTable||'CIMETIERE select '||nouveau||', * from '||nomTable||' where id in ('||ancien||')');
		
		-- Suppression.
		
		perform dede_exec('delete from '||nomTable||' where id in ('||ancien||')');
	end;
$$
language plpgsql;
comment on function dede(text, bigint, bigint, dede_champ[], text[]) is
$$DÉdoublonnage DÉcontracté
dede(nomTable text, ancien bigint, nouveau bigint, diffSaufSurColonnes text[])
Supprime l'entrée <ancien> de la table <nomTable>, au profit de l'entrée <nouveau>.

Sur les tables ayant déclaré une clé étrangère pointant sur notre colonne id, les entrées attachées à <ancien> sont reparentées à <nouveau>.
L'ancienne entrée est historisée dans <table>CIMETIERE.

Si <clesEtrangeresApplicatives> est non null, les tables ainsi listées sont considérées comme des tables liées par clé étrangère "BdD".

Si <diffSaufSurColonnes> est non null, un diff est effectué sur les deux entrées (hors les champs listés dans <diffSaufSurColonnes>):
si au moins un champ (hors ceux listés dans <diffSaufSurColonnes>) diffère, le dédoublonnage N'EST PAS effectué.

Retour: liste d'erreurs (dont les différences observées si <diffSaufSurColonnes> est définie).$$;

create or replace function dede_init(nomTable text) returns void as
$dede$
	begin
		perform dede_exec
		(
			$$
				create table $$||nomTable||'CIMETIERE'||$$ as
					select 0::bigint pivot, * from $$||nomTable||$$ limit 0;
				create function $$||nomTable||$$_dede_diff(ancien bigint, nouveau bigint, saufColonnes text[]) returns table(id bigint, err text) as
				$ddd$
					declare
						curdi refcursor;
					begin
						open curdi for select a.*, b.* from $$||nomTable||$$ a join $$||nomTable||$$ b on a.id = ancien and b.id = nouveau;
						return query select ancien, 'diff avec '||nouveau||': '||champ||': '||a||' // '||b
							from diff(curdi, saufColonnes);
						close curdi;
					end;
				$ddd$
				language plpgsql;
			$$
		);
	end;
$dede$
language plpgsql;

create or replace function dede_cascade(nomTable text, ancien bigint, nouveau bigint) returns table(id bigint, err text) as
$$
	begin
		return query
		select 0::bigint, 'Clé étrangère '||nom||' non gérée: risque de drop cascade ['||vc||' <- '||dt||'('||dc||')]'
		from dede_dependances(nomTable)
		--where type = 'c'
		;
	end;
$$
language plpgsql;

create or replace function dede_dependances(nomTable text, clesEtrangeresApplicatives dede_champ[]) returns table(nom text, type char, vs text, vt text, vc text, ds text, dt text, dc text) as
$$
	declare
		nomSchema text;
	begin
		if nomTable like '%.%' then
			nomSchema := regexp_replace(nomTable, '[.].*', '');
			nomTable := regexp_replace(nomTable, '.*[.]', '');
		end if;
		return query select
			conname::text,
			confdeltype::char,
			nomSchema,
			nomTable,
			t.relname::text,
			den.nspname::text,
			dest.relname::text,
			col.attname::text
		from pg_constraint c
			join pg_class t on t.oid = c.confrelid
			join pg_namespace en on en.oid = t.relnamespace
			join pg_class dest on dest.oid = c.conrelid
			join pg_namespace den on den.oid = dest.relnamespace
			left join unnest(c.conkey) as champs(num) on true
			left join pg_attribute col on col.attrelid = c.conrelid and col.attnum = champs.num
		where contype = 'f'
			and t.relname = nomTable
			and coalesce(en.nspname = nomSchema, true) -- Si aucun schéma n'est donné (reposant sur search_path), tant pis, on prend les tables de ce nom dans *tous* les schémas.
		;
		if clesEtrangeresApplicatives is not null then
			-- À FAIRE: pour les entrées n'ayant pas de schéma, aller chercher dans le schema_path plutôt que de prendre nomSchema.
			return query
			with l as (select * from unnest(clesEtrangeresApplicatives))
			select '-'::text, '-'::char, nomSchema, nomTable, 'id'::text, coalesce(l.schema, nomSchema), l.table, l.champ from l;
		end if;
#if defined(DEDE_CLES_ETRANGERES_APPLICATIVES)
		return query
			select '-'::text, '-'::char, c.vs, c.vt, c.vc, c.ds, c.dt, c.dc
			from DEDE_CLES_ETRANGERES_APPLICATIVES c
			where c.vs = nomSchema and c.vt = nomTable
		;
#endif
	end;
$$
language plpgsql;

create or replace function dede_dependances(nomTable text) returns table(nom text, type char, vs text, vt text, vc text, ds text, dt text, dc text) as
$$
	select * from dede_dependances(nomTable, '{}');
$$
language sql;

create or replace function dede_exec(req text) returns void as
$$
	begin
		execute req;
	end;
$$
language plpgsql;

-- Exec and REturn.
create or replace function dede_execre(req text) returns record as
$$
	begin
		return execute req;
	end;
$$
language plpgsql;

-- À FAIRE: dede_majTable en cas de modification de la table source dans le schéma: la table cimetière doit suivre.
