-- Copyright (c) 2023 Guillaume Outters
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

-- Récap d'Aberrations Détectées et Entreposage
-- Remontée d'Aberrations de Données et Entreposage

-- Deux modes d'appel:
-- - soit inclure directement le fichier à chaque besoin
-- - soit l'inclure une première fois après un #define RADE_INSTALLER 1, ce qui créera "en dur" les fonctions,
--   puis invoquer ces fonctions à chaque besoin
-- Dans les deux cas, l'invocation (du fichier ou de la fonction) est à faire:
-- - une première fois avant traitement, pour initialiser l'environnement
-- - puis à chaque fois que l'on souhaitera engranger les données recueillies
-- - dont en tout état de cause une dernière fois à la fin du script appelant

#if not defined(RADE_REF)
#define RADE_REF rade_referentiel
#endif
#if not defined(RADE_STATS)
#define RADE_STATS rade_stats
#endif
#if not defined(RADE_DETAIL)
#define RADE_DETAIL rade_detail
#endif
#if not defined(RADE_TEMP)
#define RADE_TEMP rade_temp
#endif
#if not defined(RADE_TEMP_TEMP)
-- La table de travail est-elle temporaire?
-- Inconvénient: en cas de plantage d'une tâche qui l'alimentait, on perd toute trace, il faut recommencer à 0. Et possibilité d'insérer une phase de validation visuelle sur cette table entre l'identification des cas et leur reversement vers la table persistente.
-- Avantage: isolation des tâches, chacune tourne avec sa copie de la table.
-- Par défaut non.
#define RADE_TEMP_TEMP
#else
#if :driver = "oracle"
-- /!\ La table de travail ne peut être temporaire sous Oracle.
#define RADE_TEMP_TEMP
#else
#define RADE_TEMP_TEMP temporary
#endif
#endif

#if defined(RADE_INSTALLER)
#if not defined(RADE_FONCTION)
#define RADE_FONCTION rade
#endif
#endif

#include rade_init.sql

--------------------------------------------------------------------------------
-- Exécution

-- À la première invocation (table temp inexistante), on est sur de la préparation du terrain.
-- Les fois suivantes, si la table temporaire contient des entrées, l'invocation du fichier déclenche leur affichage puis déversement vers la table persisteuse.

#if not defined(RADE_TEMP_EXISTE)
#set RADE_TEMP_EXISTE `select count(*) from pg_tables where tablename = 'RADE_TEMP'`
#endif

#if defined(RADE_INSTALLER) or !RADE_TEMP_EXISTE
#if defined(RADE_INSTALLER)
create or replace function RADE_FONCTION()
	returns void
	language plpgsql
as
$$
	begin
#endif

#if RADE_TEMP_TEMP
#include rade_init.sql
#endif

#if defined(RADE_INSTALLER)
	end;
$$;
#endif
#endif
