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

-- drop if exists manuel:
-- https://stackoverflow.com/a/26969060/1346819
-- Pourra enfin être inutile en Oracle 23c!
#endif
#define ___NOTE___ 0
#define _drop_table_if_exists(nom_table, PENDANT) \
		for rec in (select table_name from all_tables where lower(table_name) = lower(nom_table)) \
		loop \
			execute immediate 'truncate table '||rec.table_name; \
			execute immediate 'drop table '||rec.table_name; \
			PENDANT \
		end loop
#define drop_table_if_exists(nom_table) \
	begin \
		_drop_table_if_exists('nom_table',); \
	end;
#define /drop table if exists ([^ ;]*)/i drop_table_if_exists(\1)
#if 0

-- Idem pour les séquences.
#endif
#define _drop_sequence_if_exists(nom_sequence, PENDANT) <<
$$
	for rec in (select sequence_name from all_sequences where lower(sequence_name) = lower(nom_sequence))
	loop
		execute immediate 'drop sequence '||rec.sequence_name;
		PENDANT
	end loop
$$;
#define drop_sequence_if_exists(nom_sequence) \
	begin \
		_drop_sequence_if_exists('nom_sequence',); \
	end;
#define /drop sequence if exists ([^ ;]*)/i drop_sequence_if_exists(\1)
#if ___NOTE___

--------------------------------------------------------------------------------
-- Chargement en masse

-- forall équivalent à SQL*Loader (10 - 12 s), puis Import (20 s), Insert (60 s)   # http://www.dba-oracle.com/art_orafaq_data_load.htm
-- ex. forall   # https://blogs.oracle.com/connect/post/bulk-processing-with-bulk-collect-and-forall
-- CREATE TABLE tbl AS SELECT (CTAS) with PARALLEL (DEGREE x)   # https://stackoverflow.com/a/76305851/1346819
-- Table externe CSV   # https://asktom.oracle.com/ords/f?p=100:11:::::P11_QUESTION_ID:1615330671789
-- insert select aussi bon que le plus optimisé des forall   # https://livesql.oracle.com/apex/livesql/file/content_CTTBPHPAOQNA0KQ1J93TTRREB.html
-- insert /*+ append */   # https://asktom.oracle.com/ords/f?p=100:11:::::p11_question_id:1415454871121
-- insert append parallel   # https://stackoverflow.com/a/10424916/1346819
-- Un bon résumé   # https://www.bobbydurrettdba.com/2012/06/21/fast-way-to-copy-data-into-a-table/
-- Attention, plein de contraintes sur la transaction (https://www.oreilly.com/library/view/oracle-parallel-processing/156592701X/ch04s02.html)
-- mais si nous fermons la transaction ça devrait être bon.
-- Voir aussi les Index-Organized Tables (un poil des données entreposées directement dans l'index).

-- EXTASE: EXtract Table As Select, Enhanced
-- Le plus rapide et simple reste le CTAS; l'insert /*+ append */ après avoir créé la table est sensiblement équivalent mais perd petit à petit dans les grands volumes.
#endif
#define create_table_as(TABLE, SELECT) \
	create table TABLE parallel 8 nologging as\
		SELECT
#define /(?:^|\n)EXTASE\s+(\w+)\s+(.+)/s create_table_as(\1, \2)
