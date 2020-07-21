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

-- Macros de reparentement standard pour tables annexes (liées par clé étrangère) requérant un peu plus qu'un simple update pour tourner
-- (unicité empêchant de créer du doublon, etc.).

-- Reparentement (par update) avec sabordage en cas de doublon sur clé d'unicité (l'entrée qui voulait se muter se supprime si elle constate que le résultat de la mutation la rendrait semblable à une entrée existante).

create or replace function dede_repar_unique_(AUTRES_LISTE text, AUTRES_EGAL text) returns text as
$$
	select
	$f$
		with
		anciens as (select row_number() over() id, @dc, $f$||AUTRES_LISTE||$f$ from @ds.@dt where @dc = @ancien), -- Ceux qui bougent.
		existants as (delete from @ds.@dt using anciens a where a.@dc = @dt.@dc and $f$||AUTRES_EGAL||$f$ returning a.id), -- Les à reparenter pour lesquels la destination est prise (même clé, donc un reparentement donnerait une duplicate key).
		reparentes as (select a.* from anciens a left join existants e on e.id = a.id where e.id is null) -- Ceux qu'il reste à reparenter.
		update @ds.@dt set @dc = @nouveau from reparentes a where a.@dc = @dt.@dc and $f$||AUTRES_EGAL||$f$
	$f$
	;
$$
language sql;

create or replace function dede_repar_unique(a1 text) returns text as $$ select dede_repar_unique_(a1, 'a.'||a1||' = @dt.'||a1); $$ language sql;
create or replace function dede_repar_unique(a1 text, a2 text) returns text as $$ select dede_repar_unique_(a1||', '||a2, 'a.'||a1||' = @dt.'||a1||' and a.'||a2||' = @dt.'||a2); $$ language sql;

-- Reparentement (par delete + insert) avec possibilité de sabordage en cas de doublon.

create or replace function dede_repar_unique_delins_(AUTRES_LISTE text, AUTRES_EGAL text) returns text as
$$
	select
	$f$
		with
		ancien as (delete from @ds.@dt where @dc = @ancien returning *)
		insert into @ds.@dt (@dc $f$||AUTRES_LISTE||$f$)
		select @nouveau $f$||AUTRES_LISTE||$f$
		from ancien a
		where not exists(select 1 from @ds.@dt n where n.@dc = @nouveau $f$||AUTRES_EGAL||$f$)
	$f$
	;
$$
language sql;

create or replace function dede_repar_unique_delins(a1 text) returns text as $$ select dede_repar_unique_delins_(', '||a1, 'and a.'||a1||' = n.'||a1); $$ language sql;
create or replace function dede_repar_unique_delins(a1 text, a2 text) returns text as $$ select dede_repar_unique_delins_(', '||a1||', '||a2, 'and a.'||a1||' = n.'||a1||' and a.'||a2||' = n.'||a2); $$ language sql;
