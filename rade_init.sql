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

-- /!\ Fichier technique, inclus par rade.sql (qui contient toutes les définitions nécessaires). Ne pas inclure directement.
#endif

#if !defined(RADE_DEJA)
#if :driver = "pgsql"
#set RADE_DEJA `select count(*) from pg_tables where tablename = 'RADE_REF'`
#else
#set RADE_DEJA 0
#endif
#endif
#if RADE_TEMP_TEMP
#if !defined(RADE_DEJA_TEMP)
#if :driver = "pgsql"
#set RADE_DEJA_TEMP `select count(*) from pg_tables where tablename = 'RADE_TEMP'`
#else
#set RADE_DEJA_TEMP 0
#endif
#endif
#endif

-- Chez PostgreSQL, l'index doit être dépréfixé (car propriété de la table et donc dans son schéma).
-- Sous Oracle il doit être préfixé.
#if :driver = 'pgsql'
#set NOMI(x) replace(x, /.*\./, "")
#else
#set NOMI(x) x
#endif

#if !RADE_DEJA

create table RADE_REF
(
	id AUTOPRIMARY,
	indicateur T_TEXT(255),
	producteur T_TEXT(255),
	typo T_TEXT(31),
#if :driver = "oracle"
	delai_alerte interval day to second, -- not null default interval '0' second, -- En Oracle 12?
	delai_retention interval day to second -- not null default interval '36' hour -- En Oracle 12?
#else
	delai_alerte interval not null default '0s',
	delai_retention interval not null default '36h'
#endif
);
AUTOPRIMARY_INIT(RADE_REF, id)
-- À FAIRE: contrainte pour que le delai_retention soit toujours supérieur au delai_alerte (difficile d'alerter sur une info qu'on a perdue…).

create table RADE_DETAIL
(
	id T_TEXT(255),
	indicateur_id integer not null references RADE_REF(id),
	de timestamp not null,
	a timestamp,
	commentaire T_TEXT,
	statut T_TEXT(31)
);
create index NOMI(RADE_DETAIL_id_x) on RADE_DETAIL(id);
-- Pas de clé primaire, car un identifiant peut être cité pour la même erreur sur deux périodes disjointes.

create table RADE_STATS
(
	id AUTOPRIMARY,
	indicateur_id integer not null references RADE_REF(id),
	de timestamp not null,
	n integer not null
);
AUTOPRIMARY_INIT(RADE_STATS, id)

#endif

#if (!RADE_DEJA and !RADE_TEMP_TEMP) or (RADE_TEMP_TEMP and !RADE_DEJA_TEMP)
create RADE_TEMP_TEMP table RADE_TEMP
(
	q timestamp default MAINTENANT(),
	indicateur T_TEXT(255),
#if 0
	-- Si la table temporaire est évanescente, on sait qu'elle ne contiendra que du pondu par le script en cours d'exécution.
	-- Par contre si elle est partagée, il nous faut un moyen de ne pas nous marcher sur les pieds.
#endif
#if !RADE_TEMP_TEMP
	producteur T_TEXT(255),
#endif
	id T_TEXT(255),
	commentaire T_TEXT,
	statut T_TEXT(31)
);
create index NOMI(RADE_TEMP_id_x) on RADE_TEMP(id);
create index NOMI(RADE_TEMP_q_x) on RADE_TEMP(q);
create index NOMI(RADE_TEMP_statut_x) on RADE_TEMP(statut);
#if 0
-- Pour migrer de façon transparente en statut les tables qui auraient été créées avec l'ancien champ "fait":
alter table RADE_TEMP add statut T_TEXT(31);
create index NOMI(RADE_TEMP_statut_x) on RADE_TEMP(statut);
update RADE_TEMP set statut = fait where fait is not null;
-- À la sauce Oracle:
create trigger RADE_TEMP_fait_cm  before insert or update of fait on RADE_TEMP for each row when (new.fait is not null)
begin :new.statut := :new.fait; end;
#endif
#if !RADE_TEMP_TEMP
create index NOMI(RADE_TEMP_prod_x) on RADE_TEMP(producteur);
#endif
#define RADE_DEJA_TEMP 1
#endif

#define RADE_DEJA 1
