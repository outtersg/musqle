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

-- DéTrousSages
-- Supprime les trous d'entrées à partir d'une autre entrée de la même table.
-- Est considéré comme un trou une colonne à null (à moins que le null n'y soit référencé comme significatif)

-- Avant d'invoquer ce fichier, possibilité de définir:
-- DETROU_DEROULE
--   Nom d'une table entreposant le journal détaillé des détroussages.
-- DETROU_COLONNES_IGNOREES
--   Nom d'une table où paramétrer les colonnes à ignorer dans la comparaison d'entrées.
-- DETROU_COLONNES_IGNOREES_FILTRE
--   Permet de filtrer les entrées de DETROU_COLONNES_IGNOREES à considérer.
--   Si non définie, toute entrée trouvée dans DETROU_COLONNES_IGNOREES dénotera une colonne à NE PAS détrousser.
-- DETROU_COLONNES_EXPR
--   Active la possibilité de valeurs de synthèse.
--   /!\ cette synthèse de valeurs passe par l'interprétation de SQL paramétré dans la table DETROU_COLONNES_IGNOREES: les droits d'écriture sur cette table doivent donc être contrôlés.
--   Ceci permet de considérer une valeur comme nulle (et donc de la rendre détroussable par n'importe quelle autre valeur).
--   L'expression de conversion peut se référer à la table sous l'alias _source.
--   Exemple pour que dans la colonne 'nom' un '-' soit remplacé par un 'Martin':
--     -- Le case lorsqu'aucun else n'est spécifié renvoie un null:
--     insert into DETROU_COLONNES_IGNOREES (c, options) values ('nom', $$ case when _source.nom not in ('-') then _source.nom end $$);

#if defined(DETROU_COLONNES_IGNOREES)
#if `select count(*) from pg_tables where tablename = 'DETROU_COLONNES_IGNOREES'` = 0
create table DETROU_COLONNES_IGNOREES
(
	s text,
	t text,
	c text
#if defined(DETROU_COLONNES_EXPR)
	, options text
#endif
);
#endif
#endif

#if defined(DETROU_COLONNES_EXPR) and DETROU_COLONNES_EXPR == 1
#define DETROU_COLONNES_EXPR expr
#endif

-- Du hstore sans devoir s'assurer la présence de l'extension.
drop type if exists detrou_cv cascade;
create type detrou_cv as (c text, v text);

#if defined(DETROU_DEROULE)
#if `select count(*) from pg_tables where tablename = 'DETROU_DEROULE'` = 0
create table DETROU_DEROULE (q timestamp, t text, ref bigint, doublon bigint, err boolean, message text);
#endif
#endif

create or replace function detroussages(nomTable text, groupes text[], perso text) returns table(tache bigint, id bigint, info text) language plpgsql as
$$
	begin
		execute detroussages_fonc_table(nomTable, perso);
		return query select * from _detroussages_fonc(groupes);
		drop function _detroussages_fonc(groupes text[]);
	end;
$$;

create or replace function detroussages(nomTable text, ids bigint[])
	returns table(tache bigint, id bigint, info text)
	language sql
as
$$
	select * from detroussages(nomTable, array[array_to_string(ids, ' ')], null);
$$;

create or replace function detroussages(nomTable text, id0 bigint, id1 bigint) returns table(tache bigint, id bigint, info text) language sql as
$$
	select * from detroussages(nomTable, array[id0||' '||id1], null);
$$;

create or replace function detroussages_fonc_table(nomTable text, perso text) returns text language plpgsql as
$dft$
	declare
		cols text[];
		colsTraduites detrou_cv[];
	begin
#if defined(DETROU_COLONNES_EXPR)
		-- Si la table de paramétrage des colonnes spéciales possède une option de traduction de la valeur, on prend.
		select array_agg((i.c, regexp_replace(options, '^.*DETROU_COLONNES_EXPR: *', ''))::detrou_cv)
		into colsTraduites
		from DETROU_COLONNES_IGNOREES i where nomTable in (i.s||'.'||i.t, i.t)
		and options ~ '(^|,) *DETROU_COLONNES_EXPR *:';
#endif
		
		select array_agg(column_name::text) into cols from information_schema.columns
		where nomTable in (table_name, table_schema||'.'||table_name)
		and column_name not in ('id')
		and
		(
			is_nullable = 'YES'
#if defined(DETROU_COLONNES_EXPR)
			or exists(select 1 from unnest(colsTraduites) tt where tt.c = column_name)
#endif
		)
#if defined(DETROU_COLONNES_IGNOREES)
#if not defined(DETROU_COLONNES_IGNOREES_FILTRE)
#define DETROU_COLONNES_IGNOREES_FILTRE
#endif
		and column_name not in (select i.c from DETROU_COLONNES_IGNOREES i where nomTable in (i.s||'.'||i.t, i.t) DETROU_COLONNES_IGNOREES_FILTRE)
#endif
		;
		
		return regexp_replace
		(
			$$
-- À FAIRE: décoder en dur le nom de la fonction générée, afin d'éviter les interblocages entre sessions faisant simultanément des detroussages.
create or replace function _detroussages_fonc(groupes text[]) returns table(tache bigint, id bigint, info text) language sql as
$df$
#include detroussages.pg.fonc.sql
select * from maj;
$df$;
			$$,
			E'([\t]*[\n]){2,}', E'\n', 'g'
		);
	end;
$dft$;

-- La fonction reposant largement sur min() et max(), on s'assure leur présence pour tous les types standard.
#if `select count(1) from pg_aggregate a join pg_proc p on p.oid = aggfnoid join pg_type t on t.oid = aggtranstype where proname in ('min', 'max') and typname = 'bool'` < 2
-- https://stackoverflow.com/a/44004157/1346819
create aggregate max(boolean) (sfunc = boolor_statefunc, stype = boolean);
create aggregate min(boolean) (sfunc = booland_statefunc, stype = boolean);
#endif
