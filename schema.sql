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
	where table_schema not like 'pg_%' and table_schema not in ('information_schema')
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

create temporary table cles as
	-- https://stackoverflow.com/a/1152321
	select
		vc.id vc_id, dc.id dc_id
	from
		tables dt -- De Table
		join information_schema.table_constraints tc on tc.table_schema||'.'||tc.table_name = dt.nom
		join information_schema.key_column_usage kcu on tc.constraint_name = kcu.constraint_name and tc.table_schema = kcu.table_schema
		join information_schema.constraint_column_usage ccu on ccu.constraint_name = tc.constraint_name and ccu.table_schema = tc.table_schema
		join tables vt on ccu.table_schema||'.'||ccu.table_name = vt.nom -- Vers Table
		join colonnes dc on dc.table_id = dt.id and dc.nom = kcu.column_name
		join colonnes vc on vc.table_id = vt.id and vc.nom = ccu.column_name
		where tc.constraint_type = 'FOREIGN KEY'
;
alter table cles add constraint vc_id foreign key (vc_id) references colonnes(id) on delete cascade;
alter table cles add constraint dc_id foreign key (dc_id) references colonnes(id) on delete cascade;

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

create temporary table tables_aff as
	select
		id, nom, 't_'||replace(nom, '.', '_') ida,
		'[ label=<'||e'\n'||(select replace(c, '@nom', nom) from html where t = 'td') affd,
		(select c from html where t = 'tf')||e'\n'||'> ]'||e'\n' afff
	from tables
;

create temporary table colonnes_aff as
	select
		table_id, pos, id,
		(select replace(replace(c, '@nom', nom), '@type', type) from html where t = 'c') aff
	from colonnes
;

update colonnes_aff c set aff = replace(aff, '<td', '<td port="c'||pos||'"') where exists (select 1 from cles where cles.dc_id = c.id);

with
	t as
	(
		select id table_id, 0 partie, 0 pos, 0 colonne_id, ida||' '||affd descr from tables_aff
		union
		select id, 10, 0, 0, afff from tables_aff
		union
		select table_id, 1, pos, id, aff from colonnes_aff
	)
select regexp_replace(descr, '(&nbsp;| )*(<(i|font)[^>]*>(<(i|font)[^>]*>)? *(</(i|font)[^>]*>)?</(i|font)>)', '', 'g') -- Dommage, on n'a pas possibilité de faire du récursif où la seconde parenthèse (les <i></i>) fasse référence à elle-même (\2) pour traiter en une seule passe les imbrications de <i><font><b> etc.
from t order by table_id, partie, pos;

select vt.ida||' -> '||dt.ida||':c'||dc.pos
from
	cles
	join colonnes_aff vc on vc.id = cles.vc_id
	join tables_aff vt on vt.id = vc.table_id
	join colonnes_aff dc on dc.id = cles.dc_id
	join tables_aff dt on dt.id = dc.table_id
;
-- À FAIRE: ne pointer sur le bloc table que si la cible de la clé étrangère est clé primaire de ladite table. Sinon pointer sur la colonne.

select '}';
