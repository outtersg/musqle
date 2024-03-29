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

#include util.oracle.sql

-- drop if exists manuel:
-- https://stackoverflow.com/a/26969060/1346819
#define _DIE_INCR n := n + 1;
create or replace function drop_if_exists(nom_table in varchar2) return integer
as
	n integer;
-- https://stackoverflow.com/a/8729553
pragma autonomous_transaction;
begin
	n := 0;
	_drop_table_if_exists(nom_table, _DIE_INCR);
	return n;
end;
/

-- begin die('nom de la table à supprimer'); end;
-- die = Drop If Exists, évidemment.
create or replace procedure die(nom_table in varchar2) as
begin
	_drop_table_if_exists(nom_table,);
end;
/
