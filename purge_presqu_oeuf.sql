-- Copyright (c) 2022 Guillaume Outters
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

-- Vacuum full sur table de type presqu'OEUF (Objet Écrit Une Fois; WORM en anglais, Write Once Read Many).
-- Une table OEUF, ça n'est pas difficile: les anciennes entrées ne bougent plus du tout.
-- Une table presqu'OEUF, c'est plus compliqué: les entrées récentes sont susceptibles de bouger encore un peu, avant stabilisation, elle définitive.
-- On ne peut donc placer une limite claire entre les entrées présentes (figées) et les absentes (à venir).

#if !defined(REACTIVITE)
#define REACTIVITE 3
#endif

#if !defined(TABLE) || !(defined(FRANGE) || (defined(FRANGEMN) && defined(COLCREA)))
Utilisation: purge_presqu_oeuf.sql TABLE=… [COLID=…] (FRANGE=…|FRANGEMN=… COLCREA=…) [FAIRE=(1|2)]
  COLID
    Nom de la colonne portant un identifiant entier séquentiel.
    Par défaut: id
    /!\ Cette colonne doit être en autoincrément.
  FRANGE
    Nombre d''entrées susceptibles de mouvement.
  FRANGEMN
    Nombre de minutes susceptibles de mouvement.
  COLCREA
    Nom de la colonne, de type timestamp, portant la date de création de l''entrée.
	N.B.: si COLID et COLCREA sont toutes deux mentionnées, la frange sera calculée initialement sur COLCREA mais exprimée sur COLID (considérée indexée et donc plus leste pour les manipulations de masse).
  FAIRE
    Si non définie, on n''effectue que la passe préparatoire (mise de côté des données historiques).
    Si 1, la passe préparatoire est effectuée, et si elle prend moins de REACTIVITE mn, la passe définitive (purge) est effectuée dans la foulée.
	Si 2, passe préparatoire et passe définitive sont forcées.
    Il est préconisé de lancer une première fois sans l''option, puis une seconde fois avec l''option FAIRE=1 en période de moindre activité.
;
#endif

#format delim \t sans-en-tête
#silence

#define TORIG TABLE
#undef TABLE
#if !defined(COLID) and defined(FRANGE)
#define COLID id
#endif

-- À FAIRE: par date, pour les tables qui n'ont pas d'id mais ont une date créa indexée.

#set THISTO concat(TORIG, "_tmp_histo")
#set TFRANGE concat(TORIG, "_tmp_frange")

-- Ménage forcé, si nos tables de travail sont pourries (ex.: plus alignées avec la table d'origine).
#if defined(FAIRE) and FAIRE == 0
drop table if exists THISTO;
#endif
drop table if exists TFRANGE;

#define TT_VUE 1
#include tailletables.sql

#define HORO '[36m'||to_char(clock_timestamp(), 'HH24:MI:SS.MS')||'[0m '

set intervalstyle to postgres_verbose;
#set T0 `select clock_timestamp()`
select HORO||' === Purge presqu''œuf de TORIG ===';

select 'Taille actuelle: '||total from tailletables where 'TORIG' in (table_name, table_schema||'.'||table_name);

-- La première passe, ramenant tout l'historique (le "fossilisé"), peut être un peu longuette.
-- Une seconde passe prendra tout ce qui est apparu entre le début et la fin de la première passe (le "coagulé").
-- La passe définitive (dans la transaction) n'aura ainsi à se préoccuper que du minimum.
-- Ex.: avec une frange à 15 mn, si la recopie des données historiques (> 15 mn) prend 5 mn,
--      les données qui au lancement du script avaient entre 10 et 15 mn d'ancienneté (donc étaient actives, dans la frange),
--      ont maintenant entre 15 et 20 mn, donc deviennent historiques. Ce sont ces données qui sont traitées en passe 2.

#if defined(FRANGEMN)
#if defined(COLID)
#set DEBUTFRANGE `select coalesce(min(COLID), 0) from TORIG where COLCREA >= now() - interval 'FRANGEMN minutes'`
#if DEBUTFRANGE == 0
#set DEBUTFRANGE `select coalesce(max(COLID) + 1, 0) from TORIG where COLCREA < now() - interval 'FRANGEMN minutes'`
#endif
#else
ouh là;
#endif
#else
#set DEBUTFRANGE `with ids as (select COLID from TORIG order by COLID desc limit FRANGE) select coalesce(min(COLID), 0) from ids`
#endif

#if defined(COLID)
#define DANSFRANGE COLID >= DEBUTFRANGE
#else
ouh là;
#endif

select 'Frange active: '||coalesce(sum(case when DANSFRANGE then 1 end), 0)||' entrées / '||count(1) from TORIG;

select HORO||' Copie de l''historique...';
#set T0 `select clock_timestamp()`

#if `select count(1) from pg_tables where 'THISTO' in (tablename, schemaname||'.'||tablename)` == 1
#if defined(COLID)
#set DEJAFAITS `select coalesce(max(COLID), 0) from THISTO`
#define COMPLEMENTFRANGE COLID between DEJAFAITS + 1 and DEBUTFRANGE - 1
#else
ouh là;
-- À FAIRE?: en fonction de la précision de COLCREA, il y a le risque que notre précédente passe soit arrivée pile entre deux entrées de même date; l'une se retrouverait dans THISTO et l'autre non, alors il faudrait adapter en insérant un delete from THISTO where = ; insert where >= ; au lieu du seul insert where >
#endif
#bavard
insert into THISTO
select * from TORIG
where COMPLEMENTFRANGE;
#else
#bavard
create table THISTO as
select * from TORIG
where not (DANSFRANGE);
#endif
#silence

select HORO||' ... ('||(clock_timestamp() - 'T0')||')';

#if defined(FAIRE) and (FAIRE >= 2 or (FAIRE == 1 and `select case when clock_timestamp() - 'T0' < interval 'REACTIVITE minutes' then 1 else 0 end`))
#set T0 `select clock_timestamp()`

select HORO||' Obtention du verrou...';
#bavard
begin transaction;
lock table TORIG in exclusive mode; -- À FAIRE: avoir la main moins lourde sur le verrou?
#silence
select HORO||' ... ('||(clock_timestamp() - 'T0')||')';
#set T0 `select clock_timestamp()`
select HORO||' Copie de la frange active...';
#bavard
create table TFRANGE as select * from TORIG where DANSFRANGE;
#silence
select HORO||' ... ('||(clock_timestamp() - 'T0')||')';
#set T0 `select clock_timestamp()`
select HORO||' Purge!...';
#bavard
truncate table TORIG;
#silence
select HORO||' ... ('||(clock_timestamp() - 'T0')||')';
#set T0 `select clock_timestamp()`
select HORO||' Restauration de la frange active...';
#bavard
insert into TORIG select * from TFRANGE;
commit;
#silence
select HORO||' ... ('||(clock_timestamp() - 'T0')||')';
#set T0 `select clock_timestamp()`
select HORO||' Restauration de l''historique...';

#bavard
insert into TORIG select * from THISTO;
#silence

select HORO||' ... ('||(clock_timestamp() - 'T0')||')';
select 'Taille après: '||total from tailletables where 'TORIG' in (table_name, table_schema||'.'||table_name);
select HORO||' === Terminé TORIG ===';

-- À FAIRE: drop table THISTO; drop table TFRANGE;

#else

select HORO||' === Recopie de l''historique de TORIG terminé ===';
#undef FAIRE
select '[33mPour effectuer la purge, relancez en ajoutant l''option FAIRE=1[0m';

#endif
