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

-- bdd="sqlite:/tmp/bdd.sqlite3" php ../sqleur/sql2csv.php schema.sql > /tmp/1.dot && dot -Tpdf -o $D/1.pdf /tmp/1.dot

#format delim \t sans-en-tete

--------------------------------------------------------------------------------
-- Table intermédiaire

create temporary table tables as
	select row_number() over() id, table_schema||'.'||table_name nom
	from information_schema.tables
;
alter table tables add constraint tables_uid unique(id);

create temporary table colonnes as
	select
		row_number() over() id, t.id table_id, ordinal_position pos, column_name nom,
		case
			when false then ''
			else data_type||case when character_maximum_length is not null then '('||character_maximum_length||')' else '' end
		end "type"
	from information_schema.columns c
	join tables t on t.nom = c.table_schema||'.'||c.table_name
;
alter table colonnes add constraint colonnes_uid unique(id);
alter table colonnes add constraint table_id foreign key (table_id) references tables(id) on delete cascade;

update colonnes set type = replace(type, 'character varying', 'varchar');
update colonnes set type = replace(type, 'character', 'char');
update colonnes set type = replace(type, ' without time zone', ''); -- La doc de PostgreSQL explique qu'un timestamp est "without time zone" par défaut, nul besoin de le mentionner donc.
-- À FAIRE: les USER-DEFINED.

#if defined(RETRAVAIL)
#include RETRAVAIL
#endif

--------------------------------------------------------------------------------
-- Représentation Graphviz

-- On repioche dans xsif.php.
-- Grrr, pas moyen d'aligner de façon générique tout à gauche, il faut le préciser pour chaque cellule (https://gitlab.com/graphviz/graphviz/-/issues/1393
create temporary table html (t text, c text);
insert into html values ('td', '<table cellborder="1" cellspacing="0" border="0" bgcolor="#FFFFDF" color="#7F3F00">'||e'\n\t'||'<tr><td bgcolor="#7F3F00"><font color="#FFFFFF"><b>&nbsp;&nbsp;&nbsp;@nom&nbsp;&nbsp;&nbsp;</b></font></td></tr>');
insert into html values ('c', e'\t'||'<tr><td align="left">@nom&nbsp;<font point-size="9.6" color="#BF5F00"><i>@type</i></font></td></tr>');
insert into html values ('tf', '</table>');

select trim($$
digraph Schema
{
	rankdir = "LR";
	edge [ fontname="Lato" fontsize=12 ];
	node [ shape=none fontname="Lato" fontsize=12 ];
$$);

with
	t as
	(
		select
			id table_id, 0 partie, 0 pos,
			't_'||replace(nom, '.', '_')||' [ label=<'||e'\n'
			||(select replace(c, '@nom', nom) from html where t = 'td')
			descr
		from tables
		union
		select id, 10, 0, (select c from html where t = 'tf')||e'\n'||'> ]'||e'\n' from tables
		union
		select
			table_id, 1, pos,
			(select replace(replace(c, '@nom', nom), '@type', type) from html where t = 'c')
		from colonnes
	)
select regexp_replace(descr, '(&nbsp;| )*(<(i|font)[^>]*>(<(i|font)[^>]*>)? *(</(i|font)[^>]*>)?</(i|font)>)', '', 'g') -- Dommage, on n'a pas possibilité de faire du récursif où la seconde parenthèse (les <i></i>) fasse référence à elle-même (\2) pour traiter en une seule passe les imbrications de <i><font><b> etc.
from t order by table_id, partie, pos;

select '}';
