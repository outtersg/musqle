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

-- drop if exists manuel:
-- https://stackoverflow.com/a/26969060/1346819
create or replace function drop_if_exists(nom_table in varchar2) return integer
as
	n integer;
-- https://stackoverflow.com/a/8729553
pragma autonomous_transaction;
begin
	n := 0;
	for rec in (select table_name from all_tables where lower(table_name) = lower(nom_table))
	loop
		execute immediate 'truncate table '||rec.table_name;
		execute immediate 'drop table '||rec.table_name;
		n := n + 1;
	end loop;
	return n;
end;
/
