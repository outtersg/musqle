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
--   select set_config('detrou._detroussages_fonc_t_simu', null);

#include musqle.config.sql

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
-- AGRESSIF: AGRÉgation Sans Souci des Identités Faciles:
-- donne entièrement la main à la fonction d'agrégation, sans la précéder d'un case when min() = max() then max() qui permet d'optimiser les cas faciles (au détriment des valeurs null qui sont ignorées par min() et max()).
-- Avec AGRESSIF, la fonction peut donner à null une signification particulière.
#if defined(DETROU_AGREG) and !defined(DETROU_AGRESSIF)
#define DETROU_AGRESSIF 1
#endif
#if defined(DETROU_AGRESSIF) and DETROU_AGRESSIF == 1
#define DETROU_AGRESSIF agressif
#endif

#if !defined(DETROU_COLONNES_EXPR)
#define DETROU_COLONNES_EXPR 0
#endif

#if !defined(DETROU_AGREG)
#define DETROU_AGREG 0
#endif
#if !defined(DETROU_AGRESSIF)
#define DETROU_AGRESSIF 0
#endif

#if defined(DEDE_DIFF_RECESSIF) and !defined(DETROU_RECESSIF)
#define DETROU_RECESSIF DEDE_DIFF_RECESSIF
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

create or replace function detroudiff(nomTable text, groupes text[])
returns table(ida DEDE_ID_TYPE, idb DEDE_ID_TYPE, champ text, a text, b text)
language plpgsql
as
$F$
	declare
		trucs refcursor;
	begin
		-- Création d'un curseur pour transformer les groupes passés en paramètres, en prélèvement 2 à 2 côte à côte des entrées faisant partie du même groupe.
		open trucs for execute
		$$
			with
				taches as
				(
					select row_number() over() tache, string_to_array(groupe, ' ')::DEDE_ID_TYPE[] groupe
					from unnest($2) e(groupe)
				),
				entrees as
				(
					select tache, groupe[1] ida, doublon idb
					from taches, unnest(groupe) d(doublon)
					where doublon <> groupe[1]
				)
				select a.*, b.*
				from entrees l, $$||nomTable||$$ a, $$||nomTable||$$ b
				where a.DEDE_ID = l.ida and b.DEDE_ID = l.idb
		$$
		using nomTable, groupes;
		-- On exploite maintenant diff (rapide mais bête),
		-- puis sur les différences signalées par diff,
		-- on effectue un détroussages théorique pour voir si elle seraient toujours en désaccord après application des règles de convergence.
		return query
			with
				diff as (select * from :SCHEMA.diff(trucs, null, null)),
				detrou as
				(
					select tache, id, unnest(non) champ
					from :SCHEMA.detrou(nomTable, groupes, null)
					-- À FAIRE: retaper groupes pour ne considérer que ceux à doute.
				),
				deptitrou as -- dé-(Petits Tas d'Identifiants)-troussages
				(
					select array_agg(id) ids, detrou.champ
					from detrou
					group by tache, detrou.champ
				)
			select d.*
			from diff d join deptitrou d2 on d.ida = any(d2.ids) and d.idb = any(d2.ids) and d.champ = d2.champ
		;
		close trucs;
	end;
$F$;

create or replace function detroudiff(nomTable text, ida DEDE_ID_TYPE, idb DEDE_ID_TYPE)
returns table(ida DEDE_ID_TYPE, idb DEDE_ID_TYPE, champ text, a text, b text)
language sql
as
$$
	select * from :SCHEMA.detroudiff(nomTable, array[ida||' '||idb]);
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
	toutou boolean -- TOUT OU rien. Si true, les détroussages ne sont effectués que si la totalité des champs peut être alignée. Si null, mode simulation (aucun changement n'est appliqué).
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
		nomFonc := '_detroussages_fonc_'||replace(nomTable, '.', '_')||case when toutou then '_tt' when not toutou then '' else '_simu' end;
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
		agressifs detrou_cv[];
		nulls detrou_cv[];
	begin
		-- Si la table de paramétrage des colonnes spéciales possède:
		-- - une option de traduction de la valeur
		-- - une fonction d'agrégation de valeurs différentes
		-- on prend.
#for VARIABLE in colsTraduites agregats agressifs
#if VARIABLE == "colsTraduites"
#set PREFIXE DETROU_COLONNES_EXPR
#elif VARIABLE == "agregats"
#set PREFIXE DETROU_AGREG
#elif VARIABLE == "agressifs"
#set PREFIXE DETROU_AGRESSIF
#endif
#if PREFIXE
		select array_agg((i.c, regexp_replace(options, '^.*PREFIXE: *', ''))::detrou_cv)
		into VARIABLE
		from DETROU_COLONNES_IGNOREES i where nomTable in (i.s||'.'||i.t, i.t)
		and options ~ '(^|[,;]) *PREFIXE *:';
#endif
#done
#if defined(DETROU_RECESSIF)
		-- Les valeurs récessives enrichissent agregats (car elles sont agrégeables à n'importe quoi).
		with
			cvrec as
			(
				select i.c, dede_options_suffixees(options, 'DETROU_RECESSIF') v
				from DETROU_COLONNES_IGNOREES i
				where nomTable in (i.s||'.'||i.t, i.t)
			),
			crec as
			(
				select c, 'case when '||c||' not in ('||string_agg(''''||v||'''', ',')||') then '||c||' end' v
				from cvrec
				-- Les règles sur "null récessif" ne nous intéressent pas, car detrou les considère déjà naturellement récessifs.
				where v <> 'null'
				group by c
			),
			r as
			(
				select (c, format('case when min(%s) = max(%s) then min(%s) end', v, v, v))::detrou_cv t from crec
			),
			ra as (select array_agg(t) ra from r)
		-- On colle tout bonnement le tableau à la fin d'agregats. On espère qu'il n'y aura pas de cas où sont définies à la fois des valeurs récessives ET une formule d'agrégation à part: comment les combinerait-on?
		select agregats||ra into agregats from ra;
#endif
		select array_agg((i.c, regexp_replace(regexp_replace(options, '^.*(^|[,;]) *null *= *', ''), ' *([,;].*)?$', ''))::detrou_cv)
		into nulls
		from DETROU_COLONNES_IGNOREES i where nomTable in (i.s||'.'||i.t, i.t)
		and options ~ '(^|[,;]) *null *=';
		
		select array_agg(column_name::text) into cols from information_schema.columns
		where nomTable in (table_name, table_schema||'.'||table_name)
		and column_name not in ('id')
		and
		(
			is_nullable = 'YES'
#if DETROU_COLONNES_EXPR
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
#define detrou_agreg_date_floue_max(JOURS) $$ DETROU_AGRESSIF: DETROU_AGREG_DATE_FLOUE_MAX(JOURS) $$

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
#define detrou_agreg_date_floue_min_detaillee(JOURS) $$ DETROU_AGRESSIF: DETROU_AGREG_DATE_FLOUE_MIN_DETAILLEE(JOURS) $$

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
#if `select count(*) from version() where version ~ '^PostgreSQL ([0-8]\.|9\.[0-2]\.)'` == 1
				select repeat('0', 2 - length(i::text))||i i, enum[i] val
#else
				select replace(format('%2s', i), ' ', '0') i, enum[i] val
#endif
				from generate_subscripts(enum, 1) i(i)
			)
		select 
			coalesce('null = '||valNull||', ', '')||
			$$DETROU_AGRESSIF:
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
