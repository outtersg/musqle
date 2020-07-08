-- Copyright (c) 2020 Guillaume Outters
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

-- À FAIRE?: une version à appeler en select row(a.*), row(b.*): permettrait-ce d'appliquer un l.a is distinct from l.b en première passe, pour éviter de comparer champ par champ si tout se ressemble?
-- À FAIRE?: sur de gros enregistrements, on aurait peut-être intérêt à générer une fonction temporaire dédiée à l'appel, qui accède directement aux champs par leur nom. Attention, cela requérerait le point précédent (travailler par record) car le but est de se passer de json, or c'est le seul format capable de gérer deux occurrences du même nom de champ dans une entrée. Pour travailler directement sur les record il faudra que A et B arrivent non plus l'un à la suite de l'autre, mais comme deux row séparés.
-- À FAIRE: colonnes à exclure. Ce peut être via une fonction immuable sur transaction, allant chercher une table de référence (pour éviter de la recalculer pour chaque doublon à comparer).

-- Le curseur est ce qu'il y a de plus efficace, car il nous permet de faire une première passe pour récupérer le nom des colonnes, avant de boucler au plus rapide.
-- Chaque ligne de n champs doit comporter deux moitiés, chaque moitié représentant un enregistrement à comparer, dont l'ID est conventionnellement attendu en première position de la moitié.
-- Ainsi la comparaison de deux entrées A et B d'une table (id, num, descr) doit arriver sous la forme idA, numA, descrA, idB, numB, descB.
create or replace function diff(trucs refcursor) returns table(ida bigint, idb bigint, champ text, a text, b text) as
$$
	declare
		l record;
		ab json; -- On va passer par un JSON intermédiaire, qui nous explose un certain nombre de champs (on comparera sur la version texte de chaque champ de toute manière), mais a le mérite de retenir le nom des colonnes même si elles apparaissent deux fois (une pour A et une pour B).
		cols text[];
		ncols integer;
	begin
		fetch trucs into l;
		with cols as (select json_object_keys(row_to_json(l)) col)
			select array_agg(col) into cols from cols;
		ncols = array_length(cols, 1) / 2;
		cols := cols[0:ncols]; -- On ne garde que la première moitié (les champs de la seconde moitié sont supposés avoir le même nom).
		loop
			-- https://www.postgresql.org/message-id/trinity-f03554db-477f-45a8-8543-9fc5752fdec4-1399886293028%403capp-gmx-bs43
			-- https://stackoverflow.com/a/8767450/1346819
			ab := row_to_json(l);
			return query
				with tab as (select col1 - 1 as col, c, v from json_each_text(ab) with ordinality tab(c, v, col1)),
				ids as (select a.v::bigint ida, b.v::bigint idb from tab a join tab b on a.col = 0 and b.col = ncols)
				select ids.ida, ids.idb, a.c, a.v, b.v
				from tab a
				join tab b on b.col = a.col + ncols and b.v is distinct from a.v
				join ids on true
				where a.col > 0
			;
			fetch trucs into l;
			exit when not found;
		end loop;
	end;
$$
language plpgsql;
