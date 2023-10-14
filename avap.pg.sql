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

-- AVant / APrès
-- Compare les versions d'entrées d'une table avant / après une date donnée, en s'appuyant sur leur historisation préalable par ohoh().
-- Paramètres à définir:
--   TABLE
--     Deux formes possibles:
--     - table[:filtre]
--     - table1[:filtre];table2[:filtre];etc.;select … from table1, table2 …
--       En ce cas la comparaison s'effectue sur une table temporaire créée par la dernière requête, qui a à sa disposition les photos de table1, table2, etc.
--   COUPERET
--     Date à laquelle trancher (on compare ce qui était avant le couperet, à ce qui est après).

-- À FAIRE: une version proc stock.
#endif

#if defined(TABLE) && !defined(TABLES)
#define TABLES TABLE
#undef TABLE
#endif

#include musqle.config.sql
#include ohoh.config.sql

#define CREATEMP(t) create temporary table t
#define TEMP(t) pg_temp.t

-- Clé d'unicité fonctionnelle.
-- À FAIRE: permettre pour clé d'unicité autre chose que DEDE_ID (par exemple si une entrée est susceptible d'avoir été supprimée puis remplacée par une autre ayant la même clé fonctionnelle). Attention en ce cas à inclure un order by limit 1 pour ne récupérer que la première entrée pour chaque valeur de la clé fonctionnelle.
#define CLUN DEDE_ID

-- Constitution des enregistrements à comparer.
#set TABLES_SEULES replace(replace(TABLES, /:[^;]*(;|$)/, "\1"), /;[^;]* [^;]*/, "")

#for ÉTAPE in "av" "ap"

#if ÉTAPE == "av"
#define ORDRE_Q OHOH_COLQ desc
#define DU_BON_CÔTÉ <
#else
#define ORDRE_Q OHOH_COLQ asc
#define DU_BON_CÔTÉ >=
#endif

#for TABLE in split(TABLES, ";")

#if TABLE ~ /^[^:]* /

CREATEMP(ÉTAPE) as
	TABLE
;

#else

#if TABLE ~ /:/
#set FILTRE replace(TABLE, /^[^:]*:/, "")
#set TABLE replace(TABLE, /:.*/, "")
#else
#undef FILTRE
#endif
#set TABLEH concat(TABLE, OHOH_SUFFIXE)
#define OVER_Q over (partition by CLUN order by ORDRE_Q)
-- Création de la table contenant la dernière photo de chaque entrée ciblée.
CREATEMP(ÉTAPE) as
	with
		h as
		(
			select row_number() OVER_Q pos, DEDE_ID, OHOH_COLQ
			from TABLEH
			where OHOH_COLQ DU_BON_CÔTÉ 'COUPERET'
#ifdef FILTRE
			and (FILTRE)
#endif
		),
		pp as (select * from h where pos = 1) -- Plus Proche entrée.
	select h.* from pp join TABLEH h on h.DEDE_ID = pp.DEDE_ID and h.OHOH_COLQ = pp.OHOH_COLQ
;
create index on TEMP(ÉTAPE)(CLUN);

#if ÉTAPE == "ap"
-- Complément par la table actuelle (après tout, une entrée en table actuelle est bien une photo d'elle-même postérieure au couperet).
insert into TEMP(ÉTAPE)
	select OHOH_COLS_DEF, r.*
	from TABLE r
	where not exists (select 1 from TEMP(ÉTAPE) t where r.CLUN = t.CLUN)
#ifdef FILTRE
	and (FILTRE)
#endif
;
#endif

#if TABLES ~ /[ ;]/
alter table TEMP(ÉTAPE) rename to TABLE;
#endif

#endif -- #if TABLE ~ / /

#done -- #for TABLE in

#for TABLE in split(TABLES_SEULES, ";")
drop table TEMP(TABLE);
#done

#done -- av / ap

-- À FAIRE: diff ici (par detrou?)
-- Pour le moment:
-- diff -uw /tmp/av.csv /tmp/ap.csv > /tmp/avap.diff ; vi /tmp/avap.diff

select count(1) from av;
select count(1) from ap;

#sortie /tmp/av.csv
select * from av order by msisdn;
#sortie /tmp/ap.csv
select * from ap order by msisdn;
