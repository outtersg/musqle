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

#define OHOH_SUFFIXE _poubelle

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

create or replace function ohoh_(nomTable text, ancien bigint, nouveau bigint, commentaire text)
	returns void
	language plpgsql
as
$f$
	begin
		execute replace(format
		(
			$$
				insert into <nomTable>%s
					select %s, * from <nomTable> where id in ($1)
			$$,
			'OHOH_SUFFIXE',
			-- /!\ si OHOH_COLS fait référence à toto.nouveau ou la chaîne 'ancien', ça va être remplacé.
			replace(replace(replace($$OHOH_COLS$$, 'ancien', '$1'), 'nouveau', '$2'), 'commentaire', '$3')
		), '<nomTable>', nomTable)
		using ancien, nouveau, commentaire;
	end;
$f$;
