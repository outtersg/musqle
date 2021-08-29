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
-- DETROU_AGREG
--   Active la possibilité d'expressions d'agrégation.
--   Ceci permet d'agréger les valeurs différentes, par exemple si elles sont "suffisamment proches" pour être fusionnables:
--     insert into DETROU_COLONNES_IGNOREES (c, options) values ('val', $$ DETROU_AGREG: case when min(val) >= max(val) - 10 then avg(val) end $$);

-- /!\ En cas de modification de la table (ex.: ajout d'une colonne) ou de son paramétrage (ex.: par DETROU_AGREG),
-- si detroussages() a déjà été appelée, un appel post-modification (dans la même session) restera sur l'ancien schéma.
-- La prise en compte du schéma ne sera effective que sur une nouvelle session.
-- Pour forcer le recalcul de la fonction dans la même session (ex.: select * from detroussages('t'); alter table t; select * from detroussages('t');),
-- vider le cache juste après avoir effectué la modification:
--   select set_config('detrou._detroussages_fonc_t', null);
--   select set_config('detrou._detroussages_fonc_t_tt', null);

#if defined(DETROU_COLONNES_IGNOREES)
#if `select count(*) from pg_tables where tablename = 'DETROU_COLONNES_IGNOREES'` = 0
create table DETROU_COLONNES_IGNOREES
(
	s text,
	t text,
	c text
#if defined(DETROU_COLONNES_EXPR) or defined(DETROU_AGREG)
	, options text
#endif
);
#endif
#endif

#if defined(DETROU_COLONNES_EXPR) and DETROU_COLONNES_EXPR == 1
#define DETROU_COLONNES_EXPR expr
#endif

#if defined(DETROU_AGREG) and DETROU_AGREG == 1
#define DETROU_AGREG agreg
#endif

-- Du hstore sans devoir s'assurer la présence de l'extension.
drop type if exists detrou_cv cascade;
create type detrou_cv as (c text, v text);

#if defined(DETROU_DEROULE)
#if `select count(*) from pg_tables where tablename = 'DETROU_DEROULE'` = 0
create table DETROU_DEROULE (q timestamp, t text, ref bigint, doublon bigint, err boolean, message text);
#endif
#endif

-- Résidus de la version initiale proposant un troisième paramètre "perso[nnalisation]" jamais implémenté car remplacé par DETROU_COLONNES_*.
drop function if exists detroussages(text, text[], text);
drop function if exists detroussages_fonc_table(nomTable text, toutou boolean);

create or replace function detroussages(nomTable text, groupes text[], toutou boolean)
	returns table(tache bigint, id bigint, info text)
	language plpgsql
as
$$
	begin
		return query
			select d.tache, d.id,
			case
				when toutou and array_length(d.non, 1) > 0 then 'détrouable: '
				else 'détroué: '
			end||array_to_string(d.oui, ' ') info
			from detrou(nomTable, groupes, toutou) d
			where array_length(d.oui, 1) > 0;
	end;
$$;

#include current_setting.pg.sql

-- Définition de la config OHOH si sont utilisées les constantes (obsolètes) DEDE_CIMETIERE_*
#if defined(DEDE_CIMETIERE_COLS) and !defined(OHOH_COLS)
#define OHOH_COLS DEDE_CIMETIERE_COLS
#endif
#if defined(DEDE_CIMETIERE_COLS_DEF) and !defined(OHOH_COLS_DEF)
#define OHOH_COLS_DEF DEDE_CIMETIERE_COLS_DEF
#endif

-- Conversion des paramètres DETROU_CIMETIERE_* (obsolètes).
#if defined(DETROU_CIMETIERE_COLS) and !defined(DETROU_HISTO_COMM)
#define _DETROU_CIMETIERE_COLS DETROU_CIMETIERE_COLS
#undef DETROU_CIMETIERE_COLS
N'utilisez plus DETROU_CIMETIERE_COLS (valeur: "_DETROU_CIMETIERE_COLS").
À la place définissez un DETROU_HISTO_COMM pour préciser le commentaire.;
#endif
#if defined(DETROU_HISTO_COMM)
#if defined(DETROU_CIMETIERE) and not defined(OHOH_SUFFIXE)
#set OHOH_SUFFIXE DETROU_CIMETIERE
#endif
#if defined(DEDE_CIMETIERE) and not defined(OHOH_SUFFIXE)
#set OHOH_SUFFIXE DEDE_CIMETIERE
#endif
#include ohoh.pg.sql
#endif

-- Détroussages Approximatif des Doublons pour les Aligner
-- Mais bon detrou est plus explicite comme nom.
create or replace function detrou
(
	nomTable text,
	groupes text[],
	toutou boolean -- TOUT OU rien. Si true, les détroussages ne sont effectués que si la totalité des champs peut être alignée.
)
	returns table(tache bigint, id bigint, oui text[], non text[])
	language plpgsql
as
$$
	declare
		nomFonc text;
		coucou text; -- Nous indique si la fonction est cachée, parce que "Coucou? Caché!" ou l'inverse.
	begin
		-- Génération de la fonction dédiée à cette table.
		-- N.B.: perfs détaillées dans tests/detroussages.perfs.sql.
		-- À FAIRE: créer dans pg_temp?
		nomFonc := '_detroussages_fonc_'||replace(nomTable, '.', '_')||case when toutou then '_tt' else '' end;
		coucou := current_setting('detrou.'||nomFonc, true);
		if coucou is null or coucou = '' then
			execute 'drop function if exists '||nomFonc||'(text[])';
			execute detroussages_fonc_table(nomFonc, nomTable, toutou);
			execute 'set detrou.'||nomFonc||' to 1';
		end if;
		return query execute 'select * from '||nomFonc||'($1)' using groupes;
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

create or replace function detroussages_fonc_table(nomFonc text, nomTable text, toutou boolean) returns text language plpgsql as
$dft$
	declare
		cols text[];
		colsTraduites detrou_cv[];
		agregats detrou_cv[];
	begin
#if defined(DETROU_COLONNES_EXPR)
		-- Si la table de paramétrage des colonnes spéciales possède une option de traduction de la valeur, on prend.
		select array_agg((i.c, regexp_replace(options, '^.*DETROU_COLONNES_EXPR: *', ''))::detrou_cv)
		into colsTraduites
		from DETROU_COLONNES_IGNOREES i where nomTable in (i.s||'.'||i.t, i.t)
		and options ~ '(^|[,;]) *DETROU_COLONNES_EXPR *:';
#endif
#if defined(DETROU_AGREG)
		-- Si la table de paramétrage des colonnes spéciales définit la fonction d'agrégation de valeurs différentes, on prend.
		select array_agg((i.c, regexp_replace(options, '^.*DETROU_AGREG: *', ''))::detrou_cv)
		into agregats
		from DETROU_COLONNES_IGNOREES i where nomTable in (i.s||'.'||i.t, i.t)
		and options ~ '(^|[,;]) *DETROU_AGREG *:';
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
create or replace function $$||nomFonc||$$(groupes text[])
returns table(tache bigint, id bigint, oui text[], non text[])
language sql
as
$df$
#include detroussages.pg.fonc.sql
select tache, id, ouis, nons from maj join daccord using(tache);
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

--------------------------------------------------------------------------------
-- Agrégats

-- Si les dates diffèrent mais restent dans une plage de JOURS jours, la date *max* est prise comme valeur agrégée.
#define DETROU_AGREG_DATE_FLOUE_MAX(JOURS) \
	case \
		when min(COLONNE) is not distinct from max(COLONNE) then max(COLONNE) \
		when min(COLONNE) >= max(COLONNE) - interval 'JOURS days' then max(COLONNE) \
	end

-- Si les dates diffèrent mais restent dans une plage de JOURS jours, la date *min* est prise comme valeur agrégée.
-- Exception: si deux dates min émergent, le même jour mais l'une arrondie au jour l'autre avec un horodatage, c'est cette dernière, considérée plus précise, qui est choisie.
#define DETROU_AGREG_DATE_FLOUE_MIN_DETAILLEE(JOURS) \
	case \
		when min(COLONNE) is not distinct from max(COLONNE) then max(COLONNE) \
		when max(COLONNE) > min(COLONNE) + interval 'JOURS d' then null \
		when min(COLONNE)::date = min(COLONNE) then \
			case \
				when min(case when COLONNE <> COLONNE::date then COLONNE end) < min(COLONNE) + interval '1d' then min(case when COLONNE <> COLONNE::date then COLONNE end) \
				else min(COLONNE) \
			end \
		else min(COLONNE) \
	end

-- Pond l'option qui agrège selon une énumération (avec priorité croissante: le dernier élément prend le pas sur les précédents).
create or replace function detrou_agreg_enum(enum text[], valNull text)
	returns text
	immutable
	language plpgsql
as
$f$
	declare
		colonne text;
		res text;
	begin
		-- Utilisation du ! (en début de liste ASCII) pour que le max sorte de préférence toute valeur non préfixée !,
		-- c'est-à-dire non gérée: ainsi par prudence cette dernière fera échouer la fusion.
		-- Un nullif permet d'écarter ce cas, puis le substr retire le préfixe de travail.
		-- À FAIRE: si valNull, reconvertir en sortie de detrou la valeur en null.
		colonne := coalesce($$coalesce(COLONNE, '$$||valNull||$$')$$, 'COLONNE');
		with
			vals as
			(
				select replace(format('%2s', i), ' ', '0') i, enum[i] val
				from generate_subscripts(enum, 1) i(i)
			)
		select 
			$$DETROU_AGREG:
				case
					when min($$||colonne||$$) = max($$||colonne||$$) then min(COLONNE)
					when min(COLONNE) is null then null
					else
						substr
						(
							nullif
							(
								max
								(
									case $$||colonne||string_agg(format($$
										when '%s' then '!%s'||$$||colonne, val, i), '')||$$
										else 'null'
									end
								),
								'null'
							),
							4
						)
				end
			$$
		into res
		from vals;
		return res;
	end;
$f$;
