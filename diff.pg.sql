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

-- Le curseur est ce qu'il y a de plus efficace, car il nous permet de faire une première passe pour récupérer le nom des colonnes, avant de boucler au plus rapide.
-- Chaque ligne de n champs doit comporter deux moitiés, chaque moitié représentant un enregistrement à comparer, dont l'ID est conventionnellement attendu en première position de la moitié.
-- Ainsi la comparaison de deux entrées A et B d'une table (id, num, descr) doit arriver sous la forme idA, numA, descrA, idB, numB, descB.
create or replace function diff(req text) returns table(ida bigint, idb bigint, champ text, a text, b text) language plpgsql as
$$
	declare
		trucs refcursor;
	begin
		open trucs for execute req;
		return query select * from diff(trucs, null);
		close trucs;
	end;
$$;

create or replace function diff(req text, sauf text[]) returns table(ida bigint, idb bigint, champ text, a text, b text) language plpgsql as
$$
	declare
		trucs refcursor;
	begin
		open trucs for execute req;
		return query select * from diff(trucs, sauf);
		close trucs;
	end;
$$;

create or replace function diff(req text, sauf text[], nullToleres text[]) returns table(ida bigint, idb bigint, champ text, a text, b text) language plpgsql as
$$
	declare
		trucs refcursor;
	begin
		open trucs for execute req;
		return query select * from diff(trucs, sauf, nullToleres);
		close trucs;
	end;
$$;

create or replace function diff(trucs refcursor) returns table(ida bigint, idb bigint, champ text, a text, b text) as
$$
	begin
		return query select * from diff(trucs, null);
	end;
$$
language plpgsql;

-- À FAIRE?: implémenter la fonction sans nullToleres comme un simple appel à celle avec.
-- En effet les performances, crainte initiale de l'ajout de critère, ne semblent pas affectées:
-- entre -0,3 et 0,8 s de pénalité pour la version "longue" sur une requête de 13 s, en moyenne 0,6 s, soit 5%.
--   begin; select diff('select 0 id, 123 m, 456 n, 1, 456, 789', null, '{n}') from generate_series(0, 99999); select clock_timestamp() - now(); rollback;
--   (et la même chose sans le , null, '{n}' pour comparer)

create or replace function _diff_fonc(avecNullToleres boolean) returns text language sql as
$F$
	select
	$€$

-- NOTE: cette fonction ne fonctionne qu'à partir de PostgreSQL 9.3 (fonction JSON).
create or replace function diff(trucs refcursor, sauf text[]$€$||case when avecNullToleres then $€$, nullToleres text[]$€$ else '' end||$€$) returns table(ida bigint, idb bigint, champ text, a text, b text) as
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
		if sauf is not null then
			select array_agg(nom) into cols from unnest(cols) cols(nom) where not nom = any(sauf);
		end if;
		loop
			-- https://www.postgresql.org/message-id/trinity-f03554db-477f-45a8-8543-9fc5752fdec4-1399886293028%403capp-gmx-bs43
			-- https://stackoverflow.com/a/8767450/1346819
			ab := row_to_json(l);
			return query
-- À FAIRE: passer ça en pur SQL:
#if `select count(*) from version() where version ~ '^PostgreSQL ([0-8]\.|9\.[0-3]\.)'` == 1
				with tab as (select row_number() over() - 1 as col, c, v from json_each_text(ab) tab(c, v)),
#else
				with tab as (select col1 - 1 as col, c, v from json_each_text(ab) with ordinality tab(c, v, col1)),
#endif
				ids as (select a.v::bigint ida, b.v::bigint idb from tab a join tab b on a.col = 0 and b.col = ncols)
				select ids.ida, ids.idb, a.c, a.v, b.v
				from tab a
				join tab b on b.col = a.col + ncols and
				(
					b.v is distinct from a.v
					$€$||case when avecNullToleres then $€$and not (a.c = any(nullToleres) and a.v is null)$€$ else '' end||$€$
				)
				join ids on true
				where a.col > 0 and a.c = any(cols)
			;
			fetch trucs into l;
			exit when not found;
		end loop;
	end;
$$
language plpgsql;

	$€$;
$F$
;

create or replace function _diff_init() returns void language plpgsql as
$$
	begin
		execute _diff_fonc(false);
		execute _diff_fonc(true);
	end;
$$;
select _diff_init();
drop function _diff_init();
drop function _diff_fonc(boolean);

comment on function diff(trucs refcursor, sauf text[]) is
$$Renvoie la différence, champ par champ, entre deux ensembles de champs (typiquement deux entrées de la même table).

Paramètres:
	trucs
		Curseur sur une requête de type a.*, b.*, avec autant de champs dans a que dans b, et la première colonne de chaque étant forcément un entier (sera utilisé comme ID pour signaler les différences).
		Ex.:
			begin;
			declare ah cursor for select a.*, b.* from t a join t b on a.num = b.num and b.id > a.id;
			select * from diff('ah');
			close ah;
			rollback;
		Il existe une version de la fonction qui permet de passer la requête en paramètre:
			select * from diff('select a.*, b.* from t a join t b on a.num = b.num and b.id > a.id');
	sauf
		Si mentionné, exclut des champs de la comparaison.$$;
comment on function diff(trucs refcursor, sauf text[], nullToleres text[]) is
$$Renvoie la différence, champ par champ, entre deux ensembles de champs (typiquement deux entrées de la même table).
Ajoute à la version simple la possibilité d'ignorer les différences lorsqu'il s'agit entre un null sur la première partie et un non null sur la seconde.

Ex.:
diff('select 0 id, 123  n, 1, 123')                   -> (tout bon)
diff('select 0 id, 123  n, 1, 456')                   -> 1 diff avec 0: n: 456 // 123
diff('select 0 id, null::int n, 1, 456')              -> 1 diff avec 0: n: 456 // 
diff('select 0 id, null::int n, 1, 456', null, '{n}') -> (tout bon)
diff('select 0 id, 123  n, 1, null', null, '{n}')     -> 1 diff avec 0: n:  // 123
(seul le null de 0 est assimilable à la valeur de 1, sans réciprocité)$$;
