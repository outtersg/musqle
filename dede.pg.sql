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

-- Fulbert-Dédé
-- dede: DÉdoublonnage blinDÉ, ou DÉdoublonnage DÉcontracté car la fonction nous garantit qu'on ne casse rien.
-- fulbert:
--         Fusion        (deux données référençant la même réalité)
--         Unilatérale   (on ramène tout à un seul des deux enregistrements)
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
-- DEDE_CIMETIERE
--   Suffixe accolé à un nom de table pour obtenir le nom de la table d'historisation des entrées supprimées par dede.
--   La table cimetière doit posséder un certain nombre de colonnes techniques pour consignation de la suppression, suivies d'une copie des colonnes de la table d'origine (il faut donc répercuter sur la table cimetière tout ajout ou changement de colonne dans la table source).
--   La fonction dede_init() peut créer la table cimetière si elle n'existe pas.
-- DEDE_CIMETIERE_COLS, DEDE_CIMETIERE_COLS_DEF
--   Colonnes de préambule des tables DEDE_CIMETIERE.
--   Si la table d'historisation est créée par ailleurs, seul DEDE_CIMETIERE_COLS est à définir (DEDE_CIMETIERE_COLS_DEF ne sert qu'à dede_init()).
--   DEDE_CIMETIERE_COLS:
--     Valeurs à mettre dans les premières colonnes "techniques" de la table cimetière.
--     Le champ "nouveau" peut être mentionné pour obtenir l'ID de l'entrée au profit de laquelle la fusion s'effectue.
--   DEDE_CIMETIERE_COLS_DEF:
--     Définition des colonnes pour initialisation de la table via dede_init (qui fait un select DEDE_CIMETIERE_COLS_DEF, * from <table source> limit 0;).
--     Pour chaque colonne technique on mentionne donc une expression select donnant son type et son nom, ex.:
--     #define DEDE_CIMETIERE_COLS_DEF 0::bigint as id_remplacant

#define DEDE_CIMETIERE _poubelle

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

#if defined(DEDE_CIMETIERE) and not defined(DEDE_CIMETIERE_COLS)
#define DEDE_CIMETIERE_COLS nouveau
#define DEDE_CIMETIERE_COLS_DEF 0::bigint pivot
#endif

#include diff.pg.sql

drop type if exists dede_champ cascade;
create type dede_champ as ("table" text, champ text, schema text);

create or replace function dede(nomTable text, ancien bigint, nouveau bigint, detail smallint, clesEtrangeresApplicatives dede_champ[], diffSaufSurColonnes text[]) returns table(id bigint, err boolean, message text) as
$$
	declare
		curdi refcursor;
		req text;
		nullRecessifSurColonnes text[] := '{}';
	begin
		-- Vérification des données.
		
		if ancien = nouveau then
			return query select ancien, true, 'nouvel ID = ancien ID'::text;
			return;
		end if;
		if diffSaufSurColonnes is not null then
#if defined DEDE_DIFF_COLONNES_IGNOREES
			diffSaufSurColonnes := (select array_agg(i.c) from DEDE_DIFF_COLONNES_IGNOREES i where nomTable in (i.s||'.'||i.t, i.t) DEDE_DIFF_COLONNES_IGNOREES_OPTIONS)||diffSaufSurColonnes;
#if defined(DEDE_DIFF_COLONNES_RECESSIVES_VAL) or defined(DEDE_DIFF_COLONNES_RECESSIVES_FILTRE)
#if not defined(DEDE_DIFF_COLONNES_RECESSIVES_VAL)
#define DEDE_DIFF_COLONNES_RECESSIVES_VAL i.c
#endif
#if not defined(DEDE_DIFF_COLONNES_RECESSIVES_FILTRE)
#define DEDE_DIFF_COLONNES_RECESSIVES_FILTRE
#endif
			nullRecessifSurColonnes := (with val as (select DEDE_DIFF_COLONNES_RECESSIVES_VAL as val from DEDE_DIFF_COLONNES_IGNOREES i where nomTable in (i.s||'.'||i.t, i.t) DEDE_DIFF_COLONNES_RECESSIVES_FILTRE) select array_agg(val) from val)||nullRecessifSurColonnes;
#endif
#endif
			return query
				select
					idcomp ancien,
					true as err,
					'diff avec '||nouveau||': '||champ||': '||coalesce(valcomp, '<null>')||' // '||coalesce(valref, '<null>') as message
				from diffterie(nomTable, format('{"(%s,%s)"}', nouveau, ancien)::diff_ids[], diffSaufSurColonnes, nullRecessifSurColonnes)
			;
			if found then
				return;
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
		
		perform dede_exec('select '||nomTable||'DEDE_CIMETIERE('||ancien||', '||nouveau||')');
	end;
$$
language plpgsql;
comment on function dede(text, bigint, bigint, smallint, dede_champ[], text[]) is
$$DÉdoublonnage DÉcontracté
dede(nomTable text, ancien bigint, nouveau bigint, detail smallint, clesEtrangeresApplicatives dede_champ[], diffSaufSurColonnes text[])
Supprime l'entrée <ancien> de la table <nomTable>, au profit de l'entrée <nouveau>.

Sur les tables ayant déclaré une clé étrangère pointant sur notre colonne id, les entrées attachées à <ancien> sont reparentées à <nouveau>.
L'ancienne entrée est historisée dans <table>DEDE_CIMETIERE.

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

create or replace function dede_init(nomTable text) returns void as
$dede$
	begin
		perform dede_exec
		(
			$$
				create table $$||nomTable||'DEDE_CIMETIERE'||$$ as
					select DEDE_CIMETIERE_COLS_DEF, * from $$||nomTable||$$ limit 0;
				create function $$||nomTable||$$DEDE_CIMETIERE(ancien bigint, nouveau bigint) returns void language sql as
				$ddd$
					insert into $$||nomTable||$$DEDE_CIMETIERE
						select DEDE_CIMETIERE_COLS, * from $$||nomTable||$$ where id in (ancien);
					delete from $$||nomTable||$$ where id in (ancien);
				$ddd$;
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

-- La fonction suivante peut être utilisée en:
-- #define DEDE_DIFF_COLONNES_RECESSIVES_VAL diff_recessifs_options(i.c, i.options, 'récessif')
create or replace function diff_recessifs_options(champ text, options text, motCle text) returns table(recessif text) immutable language sql as
$$
	with vals as
	(
		select
			regexp_replace
			(
				(regexp_matches(options, format('[^ ,][^,]* %s|%s:[^,]*', motCle, motCle), 'g'))[1],
				format(' %s$|^%s:', motCle, motCle),
				''
			)
			val
	)
	select
		case val
			when 'null' then champ
			else champ||':'||regexp_replace(val, '^"(.*)"$', '\1')
		end
	from vals;
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
