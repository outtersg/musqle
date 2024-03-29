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

-- Fulbert / Dédé-Paul
-- dede: DÉdoublonnage blinDÉ, ou DÉdoublonnage DÉcontracté car la fonction nous garantit qu'on ne casse rien.
-- paul: Pour Assainir Une Ligne
-- fulbert:
--         Fusion        (deux données référençant la même réalité)
--         Unilatérale   (on ramène tout à un seul des deux enregistrements; ou Universelle, car générique)
--         Liens         (en particulier les entrées liées par une clé étrangère)
--       + Bretelles     (mais on historise aussi l'intégralité de l'entrée, en cas de "clé étrangère applicative" (dépendance entre tables non déclarée en base mais sur laquelle repose l'applicatif)
--     des Entrées
--         Redondantes
--   d'une Table

-- Avant d'invoquer ce fichier, possibilité de définir:
-- DEDE_DIFF_COLONNES_IGNOREES
--   Nom d'une table où paramétrer les colonnes à ignorer dans la comparaison d'entrées.
-- DEDE_DIFF_COLONNES_IGNOREES_OPTIONS
--   Filtre éventuel sur la précédente table (ex.: "and s is not null" pour ignorer les entrées sans schéma)
-- DEDE_CLES_ETRANGERES_APPLICATIVES
--   Nom d'une table où l'on pourra paramétrer des clés étrangères applicatives.
--   Comme une clé étrangère déclarée en base, Fulbert s'assurera que si une entrée A est supprimée au profit d'une entrée B avec laquelle elle faisait doublon, toute entrée d'une table tierce faisant référence à A sera reparentée vers B.
-- DEDE_REPARENTEMENTS
--   Nom d'une table où l'on pourra paramétrer les reparentements qui ne sont pas de simples update.
--   Par exemple, pour se prémunir d'un duplicate key si la table tierce possède une clé d'unicité sur le champ à reparenter, le reparentement de cette table doit en fait être un dédoublonnement.
--   Des macros pour alimenter cette table sont définies dans dede.repar.pg.sql.
-- DEDE_DEROULE
--   Nom d'une table entreposant le journal détaillé des appels à Fulbert.
-- DEDE_DETROU
--   Si définie, un détroussages est tenté pour améliorer les chances de réussite de la fusion:
--   les deux entrées sont alignées au maximum, en complétant les champs null,
--   et pour les champs différant déterminant (via configuration supplémentaire) une valeur moyennée commune.
-- Voir aussi les constantes OHOH_* dans ohoh.pg.sql (configuration de la table d'historisation des entrées supprimées).

#if defined DEDE_DIFF_COLONNES_IGNOREES
#if ! defined(DEDE_DIFF_COLONNES_IGNOREES_OPTIONS)
#define DEDE_DIFF_COLONNES_IGNOREES_OPTIONS
#endif
#if `select count(*) from pg_tables where tablename = 'DEDE_DIFF_COLONNES_IGNOREES'` = 0
create table DEDE_DIFF_COLONNES_IGNOREES
(
	s text,
	t text,
	c text
#if DEDE_DIFF_COLONNES_IGNOREES_OPTIONS
	, options text
#endif
);
#endif
#endif

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

#if defined(DEDE_REPARENTEMENTS)
#if `select count(*) from pg_tables where tablename = 'DEDE_REPARENTEMENTS'` = 0
create table DEDE_REPARENTEMENTS
(
	id serial primary key,
	champ text,
	req text
);
comment on table DEDE_REPARENTEMENTS is
$$Liste les reparentements spéciaux.
  champ
    <schema>.<table>.<champ> cible
  req
    Requête à appliquer. Y seront remplacées les variables suivantes:
      @ds Schéma de la cible
      @dt Table de la cible
      @dc Champ de la cible
      @ancien Valeur à remplacer
      @nouveau Remplacement
    Exemple de req pour éviter les duplicate key si le champ cible sert de clé unique en combinaison avec un champ 'autre':
    L'ancienne entrée est modifiée (classiquement), à moins qu'il existe déjà une entrée qui ferait doublon avec la nouvelle valeur, auquel cas l'ancienne est tout bonnement supprimée.
      with
      anciens as (select row_number() over() id, @dc, autre from @ds.@dt where @dc = @ancien), -- Ceux qui bougent.
      existants as (delete from @ds.@dt using anciens a where a.@dc = @dt.@dc and a.autre = @dt.autre returning a.id), -- Les à reparenter pour lesquels la destination est prise (même clé, donc un reparentement donnerait une duplicate key).
      reparentes as (select a.* from anciens a left join existants e on e.id = a.id where e.id is null) -- Ceux qu'il reste à reparenter.
      update @ds.@dt set @dc = @nouveau from reparentes a where a.@dc = @dt.@dc and a.autre = @dt.autre
$$;
#endif
#endif

#if defined(DEDE_DEROULE)
#if `select count(*) from pg_tables where tablename = 'DEDE_DEROULE'` = 0
create table DEDE_DEROULE (q timestamp, t text, ref bigint, doublon bigint, err boolean, message text);
#endif
#endif

-- Définition de la config OHOH si sont utilisées les constantes (obsolètes) DEDE_CIMETIERE_*
#if defined(DEDE_CIMETIERE_COLS) and !defined(OHOH_COLS)
#define OHOH_COLS DEDE_CIMETIERE_COLS
#endif
#if defined(DEDE_CIMETIERE_COLS_DEF) and !defined(OHOH_COLS_DEF)
#define OHOH_COLS_DEF DEDE_CIMETIERE_COLS_DEF
#endif

#include diff.pg.sql
#include current_setting.pg.sql
#include ohoh.pg.sql

drop type if exists dede_champ cascade;
create type dede_champ as ("table" text, champ text, schema text);

create or replace function dede(nomTable text, ancien bigint, nouveau bigint, detail smallint, clesEtrangeresApplicatives dede_champ[], diffSaufSurColonnes text[]) returns table(id bigint, err boolean, message text) as
$$
	declare
		curdi refcursor;
		req text;
		nullRecessifSurColonnes text[] := '{}';
		diffs diffterie[];
	begin
		-- Vérification des données.
		
		if ancien = nouveau then
			return query select ancien, true, 'nouvel ID = ancien ID'::text;
			return;
		end if;
		if diffSaufSurColonnes is not null then
#if defined DEDE_DIFF_COLONNES_IGNOREES
			diffSaufSurColonnes := (select array_agg(i.c) from DEDE_DIFF_COLONNES_IGNOREES i where nomTable in (i.s||'.'||i.t, i.t) DEDE_DIFF_COLONNES_IGNOREES_OPTIONS)||diffSaufSurColonnes;
#if defined(DEDE_DIFF_RECESSIF) or defined(DEDE_DIFF_COLONNES_RECESSIVES_FILTRE)
#if defined(DEDE_DIFF_RECESSIF)
#define DEDE_DIFF_COLONNES_RECESSIVES_VAL diff_recessifs_options(i.c, i.options, 'DEDE_DIFF_RECESSIF')
#else
#define DEDE_DIFF_COLONNES_RECESSIVES_VAL i.c
#endif
#if not defined(DEDE_DIFF_COLONNES_RECESSIVES_FILTRE)
#define DEDE_DIFF_COLONNES_RECESSIVES_FILTRE
#endif
			nullRecessifSurColonnes := (with val as (select DEDE_DIFF_COLONNES_RECESSIVES_VAL as val from DEDE_DIFF_COLONNES_IGNOREES i where nomTable in (i.s||'.'||i.t, i.t) DEDE_DIFF_COLONNES_RECESSIVES_FILTRE) select array_agg(val) from val)||nullRecessifSurColonnes;
#endif
#endif
			select array_agg(d) into diffs
			from diffterie(nomTable, format('{"(%s,%s)"}', nouveau, ancien)::diff_ids[], diffSaufSurColonnes, nullRecessifSurColonnes) d;
			if found then
				-- Dernière chance d'éradiquer les différences:
#if not defined(DEDE_DETROU)
				if current_setting('dede.detrou', true) = '1' then
#endif
					-- Mode détroussages: s'il existe des différences on essaie de les combler.
					-- À FAIRE?: ne pas détrouer les champs que diffterie n'aurait pas jugés importants. En effet les règles pour ignorer (diff) ou agréger quand même (detrou) ne sont pas sur le même modèle, donc detrou pourrait coincer sur un champ dont diffterie aurait dit "celui-là pas grave s'il diffère".
					with
						detrou as (select distinct unnest(oui) oui from detrou(nomTable, array[nouveau||' '||ancien], true))
					-- On soustrait du récap des incompatibilités celles qui ont pu être résolues.
					-- À noter que comme on ne compare que deux entrées, il suffit qu'une seule ait vu son champ modifié pour être sûrs de l'alignement.
					select array_agg(d) into diffs
					from unnest(diffs) d
					where not exists(select 1 from detrou where oui = champ);
#if not defined(DEDE_DETROU)
				end if;
#endif
				-- Bon, certains champs restent impossibles à concilier. On lâche l'affaire et notre appelant.
				if array_length(diffs, 1) > 0 then
			return query
				select
					idcomp ancien,
					true as err,
					'diff avec '||nouveau||': '||champ||': '||coalesce(valcomp, '<null>')||' // '||coalesce(valref, '<null>') as message
						from unnest(diffs)
			;
				return;
				end if;
			end if;
		end if;
		
		-- À partir de là on est sûrs de vouloir exécuter.
		if detail > 0 then
			return query select ancien, false, null::text;
		end if;
		
		-- Vérification des clés étrangères.
		
		if detail > 0 then
			for req in
				select 'select '||ancien||'::bigint, false, count(*)||'' '||dt||'.'||dc||' reparentés vers '||nouveau||''' from '||ds||'.'||dt||' where '||dc||'::bigint = '||ancien||' having count(*) > 0' req
				from dede_dependances(nomTable, clesEtrangeresApplicatives)
			loop
				return query execute req;
			end loop;
		end if;
		perform dede_exec
		(
			coalesce
			(
#if defined(DEDE_REPARENTEMENTS)
				replace(replace(replace(replace(replace(r.req, '@ds', ds), '@dt', dt), '@dc', dc), '@ancien', ancien::text), '@nouveau', nouveau::text),
#endif
				'update '||ds||'.'||dt||' set '||dc||' = '||nouveau||' where '||dc||'::bigint = '||ancien
			)
		)
		from dede_dependances(nomTable, clesEtrangeresApplicatives)
#if defined(DEDE_REPARENTEMENTS)
		left join DEDE_REPARENTEMENTS r on r.champ in (ds||'.'||dt||'.'||dc, dt||'.'||dc)
#endif
		;
		
		-- Historisation et suppression.
		
		perform ohoh(nomTable, ancien, nouveau);
		
		execute format
		(
			$e$
				delete from %s where id in ($1)
			$e$,
			nomTable
		) using ancien;
	end;
$$
language plpgsql;
comment on function dede(text, bigint, bigint, smallint, dede_champ[], text[]) is
$$DÉdoublonnage DÉcontracté
dede(nomTable text, ancien bigint, nouveau bigint, detail smallint, clesEtrangeresApplicatives dede_champ[], diffSaufSurColonnes text[])
Supprime l'entrée <ancien> de la table <nomTable>, au profit de l'entrée <nouveau>.

Sur les tables ayant déclaré une clé étrangère pointant sur notre colonne id, les entrées attachées à <ancien> sont reparentées à <nouveau>.
L'ancienne entrée est historisée dans <table>OHOH_SUFFIXE.

Si <detail> > 0, des informations sont renvoyées quant au reprisage effectué.

Si <clesEtrangeresApplicatives> est non null, les tables ainsi listées sont considérées comme des tables liées par clé étrangère "BdD".

Si <diffSaufSurColonnes> est non null, un diff est effectué sur les deux entrées (hors les champs listés dans <diffSaufSurColonnes>):
si au moins un champ (hors ceux listés dans <diffSaufSurColonnes>) diffère, le dédoublonnage N'EST PAS effectué.

Retour: liste de (<id>, <err>, <message>):
- erreurs ayant empêché la fusion (dont les différences observées si <diffSaufSurColonnes> est définie) [<err> = true]
- détail des opérations si demandé [<err> = false]$$;

create or replace function dede(nomTable text, ancien bigint, nouveau bigint, clesEtrangeresApplicatives dede_champ[], diffSaufSurColonnes text[]) returns table(id bigint, err text) as
$$
	select id, message
	from dede(nomTable, ancien::bigint, nouveau::bigint, 0::smallint, clesEtrangeresApplicatives, diffSaufSurColonnes)
	where err;
$$
language sql;
create or replace function dede(nomTable text, ancien integer, nouveau integer, clesEtrangeresApplicatives dede_champ[], diffSaufSurColonnes text[]) returns table(id bigint, err text) as
$$
	select dede(nomTable, ancien::bigint, nouveau::bigint, clesEtrangeresApplicatives, diffSaufSurColonnes);
$$
language sql;

create or replace function dedede(nomTable text, ancien bigint, nouveau bigint) returns setof bigint as
$$
	with d as
	(
		select * from dede(nomTable, ancien, nouveau, 1::smallint, null, '{}')
	),
#if defined(DEDE_DEROULE)
	de as
	(
		insert into DEDE_DEROULE select clock_timestamp(), nomTable, nouveau, d.* from d returning ref, doublon, err
	)
#else
	de as (select err from d where err)
#endif
	select distinct d.id
	from d left join de on de.err
	where de.err is null;
$$
language sql;

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
			tcol.attname::text,
			den.nspname::text,
			dest.relname::text,
			col.attname::text
		from pg_constraint c
			join pg_class t on t.oid = c.confrelid
			join pg_namespace en on en.oid = t.relnamespace
			join pg_attribute tcol on tcol.attrelid = c.confrelid and tcol.attnum = any(c.confkey)
			join pg_class dest on dest.oid = c.conrelid
			join pg_namespace den on den.oid = dest.relnamespace
			join pg_attribute col on col.attrelid = c.conrelid and col.attnum = any(c.conkey)
		where contype = 'f'
			and t.relname = nomTable
			and (nomSchema is null or en.nspname = nomSchema) -- Si aucun schéma n'est donné (reposant sur search_path), tant pis, on prend les tables de ce nom dans *tous* les schémas.
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
			where (nomSchema is null or c.vs = nomSchema) and c.vt = nomTable
		;
#endif
	end;
$$
language plpgsql stable;

create or replace function dede_dependances(nomTable text) returns table(nom text, type char, vs text, vt text, vc text, ds text, dt text, dc text) as
$$
	select * from dede_dependances(nomTable, '{}');
$$
language sql stable;

create or replace function dede_exec(req text) returns void as
$$
	begin
		execute req;
	end;
$$
language plpgsql;

create or replace function dede_options_suffixees(options text, motCle text) returns table(val text) immutable language sql as
$$
		select
			regexp_replace
			(
				(regexp_matches(options, format('[^ ,][^,]* %s|%s:[^,]*', motCle, motCle), 'g'))[1],
				format(' %s$|^%s:', motCle, motCle),
				''
			)
			val;
$$;

create or replace function diff_recessifs_options(champ text, options text, motCle text) returns table(recessif text) immutable language sql as
$$
	select
		case val
			when 'null' then champ
			else champ||':'||regexp_replace(val, '^"(.*)"$', '\1')
		end
	from dede_options_suffixees(options, motCle) vals;
$$;

#if 0
-- Ce qui suit ne fonctionne pas: a column definition list is required for functions returning "record"
-- Exec and REturn.
create or replace function dede_execre(req text) returns setof record as
$$
	begin
		return query execute req;
	end;
$$
language plpgsql;
#endif

-- À FAIRE: dede_majTable en cas de modification de la table source dans le schéma: la table cimetière doit suivre.
