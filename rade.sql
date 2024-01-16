#if 0
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

-- N.B.: ce fichier étant destiné à être inclus, l'ensemble de ses commentaires est encadré de #if 0 pour éviter de polluer l'incluant de nos notes.

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
#endif

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
-- RADE_DEDOU: pour n'entrer chaque identifiant qu'une seule fois par passe. Par défaut à 1.
#if defined(RADE_DEDOU)
#define _RADE_DEDOU RADE_DEDOU
#else
#define _RADE_DEDOU 1
#endif

#if defined(RADE_INSTALLER)
#if not defined(RADE_FONCTION)
#define RADE_FONCTION rade
#endif
#endif
#if not defined(RADE_DEJA)
#if defined(RADE_INSTALLER)
#define RADE_DEJA 0
#else
#define RADE_DEJA 1
#endif
#endif

#include rade_init.sql

--------------------------------------------------------------------------------
-- Exécution

-- À la première invocation (table temp inexistante), on est sur de la préparation du terrain.
-- Les fois suivantes, si la table temporaire contient des entrées, l'invocation du fichier déclenche leur affichage puis déversement vers la table persisteuse.

#if RADE_TEMP_TEMP

#define MIENS 1=1
#define AND_MIENS
#define MIENS_AND
#define WHERE_MIENS
#if not defined(RADE_TEMP_EXISTE)
#if :driver = "pgsql"
#set RADE_TEMP_EXISTE `select count(*) from pg_tables where tablename = 'RADE_TEMP'`
#else
-- Si pas moyen de détecter, on suppose que tout est en place.
#set RADE_TEMP_EXISTE 1
#endif
#endif

#else

#define MIENS producteur = ':SCRIPT_NAME'
#define AND_MIENS and MIENS
#define MIENS_AND MIENS and
#define WHERE_MIENS where MIENS

#endif

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

#if RADE_TEMP_TEMP
#define RADE_T_PRODUCTEUR ':SCRIPT_NAME'
#else
#define RADE_T_PRODUCTEUR t.producteur
#endif
#define RADE_REF_POUR_T RADE_REF r where r.indicateur = t.indicateur and r.producteur = RADE_T_PRODUCTEUR
#define T_POUR_D where t.id = d.id and t.indicateur = cast(d.indicateur_id as T_TEXT(255))
#define RADE_TEMP_POUR_D RADE_TEMP t T_POUR_D

-- À appeler depuis une table externe pour savoir si cle a déjà été détectée en tant qu'indic.
#define RADE_NOUVEAU(cle, indic) \
	not exists (select 1 from RADE_DETAIL h__ where h__.indicateur_id in (select r__.id from RADE_REF r__ where r__.producteur = ':SCRIPT_NAME' and r__.indicateur = indic) and h__.id = cle)

insert into RADE_REF (indicateur, producteur)
	select distinct indicateur, RADE_T_PRODUCTEUR
	from RADE_TEMP t
	where MIENS_AND not exists (select 1 from RADE_REF_POUR_T)
;
update RADE_TEMP t set indicateur = (select max(id) from RADE_REF_POUR_T) WHERE_MIENS;

update RADE_DETAIL d
set
	a = MAINTENANT()
#if 0
	-- À FAIRE? Actualiser le commentaire? Par concaténation, uniquement si vide?
#endif
where exists (select 1 from RADE_TEMP_POUR_D)
#if 0
-- Si notre ID a été touché plusieurs fois, on n'élargit que sa dernière occurrence.
-- À FAIRE: tenir compte aussi du delai_retention: une entrée historique (a < MAINTENANT() - delai_retention) ne doit pas être prolongée, mais une nouvelle doit être créée.
#endif
and not exists (select 1 from RADE_DETAIL recent where recent.id = d.id and recent.indicateur_id = d.indicateur_id and recent.de > d.de)
;
delete from RADE_TEMP t where MIENS and exists (select 1 from RADE_DETAIL d T_POUR_D);

insert into RADE_DETAIL (indicateur_id, id, a, de, commentaire)
	select
		cast(t.indicateur as integer), t.id, MAINTENANT(),
#if _RADE_DEDOU
		min(t.q), max(t.commentaire)
#else
		t.q, t.commentaire
#endif
	from RADE_TEMP t WHERE_MIENS
#if _RADE_DEDOU
	group by t.indicateur, t.id
#endif
;
delete from RADE_TEMP WHERE_MIENS;

#if defined(RADE_INSTALLER)
	end;
$$;
#endif
