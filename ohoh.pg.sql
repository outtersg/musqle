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

-- Arthur / Oh
-- oh: Opérations d'Historisation
-- arthur:
--         Archivage
--     des Révisions
--   d'une Table              (de ses entrées)
-- à but d'Historique,        (cas d'usage 1: traçabilité: savoir ce qu'il s'est passé sur les entrées)
--       d'Ultra-persistence, (cas d'usage 2: pouvoir garder éternellement les valeurs)
--   ou de Restauration       (cas d'usage 3: pouvoir restaurer intégralement ce qui avait été malencontreusement supprimé)

-- Configuration ---------------------------------------------------------------
-- Avant d'invoquer ce fichier, possibilité de définir:

#if 0
-- OHOH_SUFFIXE
--   Suffixe accolé à un nom de table pour obtenir le nom de la table d'historisation.
--   La table cimetière doit posséder un certain nombre de colonnes techniques pour consignation de la suppression, suivies d'une copie des colonnes de la table d'origine (il faut donc répercuter sur la table cimetière tout ajout ou changement de colonne dans la table source).
--   La table cimetière est créée automatiquement si elle n'existe pas au moment de l'appel à ohoh().
-- OHOH_COLS, OHOH_COLS_DEF
--   Colonnes de préambule des tables OHOH_SUFFIXE.
--   Si la table d'historisation est créée par ailleurs, seule OHOH_COLS est à définir.
--   OHOH_COLS:
--     Valeurs à mettre dans les premières colonnes "techniques" de la table cimetière.
--     Le champ "nouveau" peut être mentionné pour obtenir l'ID de l'entrée au profit de laquelle la fusion s'effectue.
--   OHOH_COLS_DEF:
--     Définition des colonnes pour initialisation de la table cimetière:
--       create table <table_source>_poubelle as
--         select OHOH_COLS_DEF, * from <table source> limit 0;
--     Pour chaque colonne technique on mentionne donc une expression select donnant son type et son nom, ex.:
--     #define OHOH_COLS_DEF 0::bigint as id_remplacant
-- OHOH_COND
--   Si définie, des conditions supplémentaires s'appliquent à l'historisation.
--   Ex.:
--     #define OHOH_COND id > 0
-- OHOH_MAJ
--   Si définie, est d'abord tentée le recyclage d'une entrée d'historique.
--   Ex. pour recycler si une historisation de la même entrée a déjà été faite dans les 5 dernières minutes:
--     #define OHOH_COLS_DEF 0::bigint remplace_par bigint, now() date_histo, 0::int nrevs
--     #define OHOH_COLS     nouveau, clock_timestamp(), 0
--     #define OHOH_MAJ      , nrevs = nrevs + 1 where date_histo >= now() - interval('5 minutes')
#endif

#include ohoh.config.sql

#if defined(OHOH_SUFFIXE) and not defined(OHOH_COLS)
#define OHOH_COLS nouveau
#define OHOH_COLS_DEF 0::bigint pivot
#endif

create or replace function ohoh(nomTable text, ancien bigint, nouveau bigint)
	returns void
	language plpgsql
as
$$
	begin
		perform ohoh(nomTable, ancien, nouveau, null);
	end;
$$;

create or replace function ohoh(nomTable text, ancien bigint, nouveau bigint, commentaire text)
	returns void
	language plpgsql
as
$f$
	begin
		perform ohoh_(nomTable, ancien, nouveau, commentaire);
	exception when undefined_table then
		execute format
		(
			$$
				create table %s%s as
					select %s, * from %s limit 0;
			$$,
			nomTable,
			'OHOH_SUFFIXE',
			$$OHOH_COLS_DEF$$,
			nomTable
		);
		perform ohoh_(nomTable, ancien, nouveau, commentaire);
	end;
$f$;

-- À FAIRE: #if defined(OHOH_ MAJ) mais celle-ci n'est pas de la forme ", <champ> = <val> where <cond>", erreur.

-- Fonction de création de la fonction ohoh_.
-- S'agissant de nombreux replace() sur une expression statique (#define etc.), on a en effet intérêt à effectuer ces replace() à la construction plutôt qu'à chaque exécution (même si l'optimiseur PostgreSQL saura les calculer une seule fois, c'est toujours une de trop, de plus d'un point de vue lisibilité de la fonction définitive on y gagne).

#define OHOH_CORPS_RETRAITE_DEBUT \
	$€$||regexp_replace(regexp_replace(regexp_replace($€$
#define OHOH_CORPS_RETRAITE_FIN \
	$€$, \
		'([^.])ancien', '\1$1', 'g'), \
		'([^.])nouveau', '\1$2', 'g'), \
		'([^.])commentaire', '\1$3', 'g') \
	||$€$

create or replace function pg_temp.ohoh_fonc()
	returns void
	language plpgsql
as
$fof$
	begin
		execute regexp_replace($€$

create or replace function ohoh_(nomTable text, ancien bigint, nouveau bigint, commentaire text)
	returns void
	language plpgsql
as
$f$
	begin
		execute
			replace
		(
			$$
				OHOH_CORPS_RETRAITE_DEBUT
#if defined(OHOH_MAJ)
				with
					u as
					(
						update <nomTable>OHOH_SUFFIXE
						set id = id OHOH_MAJ and id = $1
						returning id
					)
#endif
				insert into <nomTable>OHOH_SUFFIXE
					select OHOH_COLS, * from <nomTable> where id in ($1)
#if defined(OHOH_COND)
					and OHOH_COND
#endif
#if defined(OHOH_MAJ)
					and not exists(select 1 from u)
#endif

				OHOH_CORPS_RETRAITE_FIN
				$$,
				'<nomTable>', nomTable
			)
		using ancien, nouveau, commentaire;
	end;
$f$;

			$€$, E'(^|\n)([ \t]*(\n|$))+', '\1', 'g')
		;
	end;
$fof$;
select pg_temp.ohoh_fonc();
