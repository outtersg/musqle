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
