#define ___NOTE___ 0
#if ___NOTE___
-- Copyright (c) 2024 Guillaume Outters
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

-- Requêtes Anormalement Coincées: Listage, Extraction, Toilettage, Transposition et Enregistrement
-- Requêtes Agaçantes et Longues: Énumération, Uniformisation, Synthèse et Enregistrement

#endif
#if !defined(RACLETTE_TABLE)
#define RACLETTE_TABLE t_req_longues
#endif
#if !defined(RACLETTE_BOULOT)
#define RACLETTE_BOULOT t_req_longues_tmp
#endif
#if !defined(RACLETTE_TEMP)
#define RACLETTE_TEMP
#endif

#define RACLETTE_CLÉ_T 128
#define RACLETTE_TÊTE_REQ_T 30

#if defined(:pilote) && :pilote ~ /^ora/
#define RACLETTE_MÉM_T clob
#if ___NOTE___
-- Pensez à avoir installé la fonction crc_lob de crc.oracle.sql
#endif
#define RACLETTE_CRC(x) crc_lob(x)
#else
#define RACLETTE_MÉM_T text
#define RACLETTE_CRC(x) md5(x)
#endif

#if 0
create table RACLETTE_TABLE
(
	cle T_TEXT(RACLETTE_CLÉ_T),
	req RACLETTE_MÉM_T
);
-- À FAIRE?: une colonne "assumé", pour dire que pas la peine de s'alarmer sur cette requête.
#endif

drop table if exists RACLETTE_BOULOT;
create RACLETTE_TEMP table RACLETTE_BOULOT as
#if defined(:pilote) && :pilote ~ /^ora/
#if ___NOTE___
-- Pour Oracle, comment différencier:
-- - Le même sql_id indique simplement deux requêtes qui jouent le même SQL
-- - fixed_table_sequence est propre à une requête, mais il est instable (il monte au fur et à mesure que la requête franchit des jalons internes Oracle; et même à l'intérieur d'une requête en parallèle, toutes n'en sont pas au même stade)
-- - audsid semble stable, et partagé par tous les exécutants de la même requête. Mais il n'est pas propre à la requête (il est préservé d'une requête sur l'autre dans la session, donc ne permet pas de distinguer deux lancements de la même requête dans la même session).
--   (les audsid = 0 ne nous embêtent pas car ils concernent uniquement les sql_id is null et username is null, que nous excluons)
-- - L'audsid étant recyclable, il lui faut aussi le préfixe de la date
-- - Le sql_exec_id démarre à 16777216 (2^24), et croît au fil des usages.
--   Mais une même requête jouée en parallèle a mêmes sql_exec_id et sql_id (c'est ce qu'on veut).
-- => Pour identifier une requête unique, on utilise le triplet <date>.<audsid>.<sql_exec_id>
#endif
	with
		e as
		(
			select sql_id, audsid, sql_exec_id, min(sql_exec_start) sql_exec_start, username
			from v$session
			where sql_exec_start < sysdate - interval '10' minute
			and status = 'ACTIVE' and username not in ('SYS')
			group by sql_id, audsid, sql_exec_id, username
		),
		-- Le SQL figure en plusieurs exemplaires en fonction de je ne sais quoi.
		su as (select s.sql_id, min(child_number) micn from e, v$sql s where s.sql_id = e.sql_id group by s.sql_id),
		s as
		(
			select e.*, sql_fulltext req
			from e, su, v$sql s
			where su.sql_id = e.sql_id and s.sql_id = su.sql_id and s.child_number = micn
		)
	select
		cast('' as varchar2(RACLETTE_CLÉ_T)) cle, -- La clé sera calculée plus tard après purge de la requête.
		-- On utilise un modulo pour raccourcir un peu; le risque de collision est faible car nous intéressant aux requêtes longues on espère qu'elles ne sont pas jouées 1 M fois dans la journée…
		to_char(sql_exec_start, 'YYYYMMDD')||'.'||audsid||'.'||mod(sql_exec_id, 1048576) id,
		sql_exec_start debut,
		req,
		'$USER = '||username||
		(
			select
				case
					when count(1) = 0 then ''
					else ' | '||listagg(p.name||' = '||p.value_string, ' | ') within group (order by position, dup_position)
				end
			from v$sql_bind_capture p where p.sql_id = s.sql_id
		)
		params
	from s
;
#else
	À FAIRE;
#endif

#define RACLETTE_EXTRAIRE_PARAM(EXPR, REMPL) <<
$$
	update RACLETTE_BOULOT
	set
		req = regexp_replace(regexp_replace(req, EXPR, REMPL), '(\$<[^:>]*):[^>]*(>)', '\1\2'),
		params =
			regexp_replace
			(
				params||regexp_replace
				(
					regexp_replace
					(
						regexp_replace(req, EXPR, REMPL),
						'(\$<[^:>]*):([^>]*)(>)',
						'\1\3 = \2'
					),
					'^[^]*|[^]*|[^]*$',
					' | '
				),
				'^ \| | \| $',
				''
			)
	where regexp_like(req, EXPR)
$$;
#if ___NOTE___

-- Vous pouvez définir un RACLETTE_INCL qui sera invoqué ici:
-- il peut contenir des règles d'extraction de parties variables pour faire converger les requêtes vers un patron unique.
-- Ex.:
-- RACLETTE_EXTRAIRE_PARAM('clé = ''(valeur)''', 'clé = $<clé:\1>');

#endif ___NOTE___
#ifdef RACLETTE_INCL
#include RACLETTE_INCL
#endif

#if ___NOTE___
-- On calcule notre clé SQL.
-- On n'utilise *pas* la clé d'unicité de la base (ex.: sql_hash_value ou sql_id sous Oracle),
-- car elle est calculée sur le SQL original, alors qu'avec nos extractions on a pu encore épurer le SQL.
#endif

-- Cette version est trop longue:
--#define REQ_À_PLAT regexp_replace(regexp_replace(req, '(--.*)?($|'||chr(10)||')', ' '), '  +', ' ')
#define REQ_À_PLAT regexp_replace(replace(replace(req, chr(10), ' '), chr(13), ' '), '  +', ' ')

update RACLETTE_BOULOT
set
	cle =
		'['||RACLETTE_CRC(req)||'] '
		||case
			when length(REQ_À_PLAT) < RACLETTE_CLÉ_T - 35 then REQ_À_PLAT
			else substr(REQ_À_PLAT, 1, RACLETTE_TÊTE_REQ_T)||' […] '||substr(REQ_À_PLAT, 1 + length(REQ_À_PLAT) - (RACLETTE_CLÉ_T - 40 - RACLETTE_TÊTE_REQ_T))
		end
;

insert into RACLETTE_TABLE (cle, req)
	-- Le CLOB est un peu chiant, on ne peut faire de comparaison, distinct, group by dessus.
	-- En cas de plusieurs fois la même requête en train de tourner, on se montre inventif pour n'en insérer qu'un.
	with n as
	(
		select cle, req, row_number() over (partition by cle order by 1) pos
		from RACLETTE_BOULOT t
		where not exists (select 1 from RACLETTE_TABLE r where r.cle = t.cle)
	)
	select cle, req from n where pos = 1
;

#include rade.sql

insert into RADE_TEMP (RADE_TEMP_PRODUC_COL, indicateur, id, q, commentaire)
	select RADE_TEMP_PRODUC_VAL, cle, id, debut, params
	from RACLETTE_BOULOT
;

#include rade.sql
