-- Copyright (c) 2021 Guillaume Outters
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

-- DéTrousSages
-- Supprime les trous d'entrées à partir d'une autre entrée de la même table.
-- Est considéré comme un trou une colonne à null (à moins que le null n'y soit référencé comme significatif)

#if defined(DETROU_COLONNES_IGNOREES)
#if `select count(*) from pg_tables where tablename = 'DETROU_COLONNES_IGNOREES'` = 0
create table DETROU_COLONNES_IGNOREES
(
	s text,
	t text,
	c text
);
#endif
#endif

create or replace function detroussages(nomTable text, groupes text[], perso text) returns table(tache bigint, id bigint, info text) language plpgsql as
$$
	begin
		execute detroussages_fonc_table(nomTable, perso);
		return query select * from _detroussages_fonc(groupes);
		drop function _detroussages_fonc(groupes text[]);
	end;
$$;

create or replace function detroussages_fonc_table(nomTable text, perso text) returns text language plpgsql as
$dft$
	declare
		cols text[];
	begin
		select array_agg(column_name::text) into cols from information_schema.columns
		where nomTable in (table_name, table_schema||'.'||table_name)
		and is_nullable = 'YES'
		and column_name not in ('id')
#if defined(DETROU_COLONNES_IGNOREES)
		and column_name not in (select i.c from DETROU_COLONNES_IGNOREES i where nomTable in (i.s||'.'||i.t, i.t))
#endif
		-- À FAIRE: permettre, colonne par colonne, d'avoir une autre valeur "insignifiante" (ex.: '', '-').
		;
		
		return
			$$
-- À FAIRE: décoder en dur le nom de la fonction générée, afin d'éviter les interblocages entre sessions faisant simultanément des detroussages.
create or replace function _detroussages_fonc(groupes text[]) returns table(tache bigint, id bigint, info text) language sql as
$df$
#include detroussages.pg.fonc.sql
select * from maj;
$df$;
			$$;
	end;
$dft$;
