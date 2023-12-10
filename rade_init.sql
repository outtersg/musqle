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
	commentaire T_TEXT
);
create index RADE_DETAIL_id_x on RADE_DETAIL(id);
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
	id T_TEXT(255),
	commentaire T_TEXT,
	fait T_TEXT(31)
);
create index RADE_TEMP_fait_x on RADE_TEMP(fait);
#define RADE_DEJA_TEMP 1
#endif

#define RADE_DEJA 1
