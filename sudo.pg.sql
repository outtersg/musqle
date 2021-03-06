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

#if 0
--------------------------------------------------------------------------------
-- Pseudo-sudo
--
-- Exemple d'utilisation:
-- permettre à n'importe qui de créer toute extension (de nom pas trop louche) du moment qu'il l'attache à un schéma qu'il détient.
insert into admin.sudoers (command, cond) values ('create extension [a-z]+ schema ([_a-z]+)', 'exists(select 1 from information_schema.schemata s where s.schema_name = ''\1'' and s.schema_owner = session_user)');
-- notons qu'en remplaçant le premier [a-z]+ par une liste blanche prédéfinie, on émuler les "trusted extensions" de PostgreSQL 13.
--------------------------------------------------------------------------------
-- Test du create extension ci-dessus:
-- Constitution du jeu de test:
create user toto password 'toto';
create schema toto;
alter schema toto owner to toto;
-- Et pour tester, en PASSWORD=toto psql -U toto:
create extension hstore schema toto; -- Plante en "must be superuser".
select admin.sudo('create extension hstore schema public'); -- Renvoie false.
select admin.sudo('create extension hstore schema toto'); -- Renvoie true.
select admin.sudo('create extension hstore schema toto'); -- Pète en "already exists", montrant que la précédente est passée.
-- Puis évidemment un peu de ménage en superuser:
drop schema toto;
drop user toto;
--------------------------------------------------------------------------------
#endif

-- Emplacement de la table de paramétrage ("sudoers"):
-- Veillez à utiliser un schéma sur lequel seul le super-utilisateur peut taper (insert, mais aussi drop / create).
#if !defined(SUDOERS)
#define SUDOERS admin.sudoers
#set SUDOERS concat("'", SUDOERS, "'")
#endif
#define SUDO_TRACES null

-- À FAIRE: traces. Bien noter tout de même que le système (de trace dans une table) est faillible, puisqu'il suffit à l'appelant de rollbacker pour ne pas être fliqué (l'entrée en table traces disparaîtrait avec le rollback).
-- À FAIRE: possibilité de renvoyer le résultat d'un sudo select?

-- Reposant sur des fonctions temporaires pour construire notre environnement, on doit se prémunir d'une concurrence critique où notre fonction, potentiellement dans public, serait remplacée par un petit malin avant qu'on ne l'appelle: donc on transactionne.
begin;

create or replace function _sudo_def(nomFonctionSudo text, nomFonctionExecBooleen text, nomTableSudoers text, nomTableTraces text) returns text language sql as
$bla$
	select
	$$
		-- Balancer une exception eût été mieux, cependant cela nous garantit qu'on perd notre trace (rollback implicite).
		-- Alors qu'en renvoyant un booléen, la trace n'est perdue que si l'appelant oublie de rollbacker sur retour false.
		-- L'appelant a donc toujours la possibilité de se masquer, mais uniquement sur furtivité volontaire (certes ce sont ceux-là qu'on voudrait intercepter avant tout…).
		create or replace function $$||nomFonctionSudo||$$(commande text) returns boolean language plpgsql SECURITY DEFINER as
		$corps$
			declare
				valide text;
				cond text;
			begin
				with v as
				(
					select
						regexp_matches(commande, '^'||s.command||'$', coalesce(s.flags, '')) valide,
						'^'||s.command||'$' expr,
						s.cond
					from $$||nomTableSudoers||$$ s
				)
				select commande
				into valide
				from v
				where v.cond is null or $$||nomFonctionExecBooleen||$$(regexp_replace(commande, v.expr, v.cond))
				limit 1;
				if not found then return false; end if;
				
				execute valide;
				
				return true;
			end;
		$corps$;
		
		create or replace function $$||nomFonctionExecBooleen||$$(commande text) returns boolean language plpgsql as
		$corps$
			declare
				res boolean;
			begin
				execute format('select %s', commande) into res;
				return res;
			end;
		$corps$;
	$$;
$bla$;

create or replace function _sudo_installer(nomTableSudoers text, nomTableTraces text) returns void language plpgsql as
$$
	declare
		nomSchema text;
	begin
		-- À FAIRE: si nomTable ne contient pas de ., le préfixer du premier composant du search_path. Mais bon ce n'est pas bien de passer par un nom non qualifié: ça pourrait valoir un raise exception.
		select _sudo_verifierSchemaDe(nomTableSudoers) into nomSchema;
		perform _sudo_verifierTable(nomTableSudoers, 'command text, flags text, cond text');
		execute _sudo_def(nomSchema||'.sudo', nomSchema||'.execb', nomTableSudoers, nomTableTraces);
	end;
$$;

create or replace function _sudo_verifierSchemaDe(nomTable text) returns text language plpgsql as
$$
	declare
		nomSchema text;
	begin
		select split_part(nomTable, '.', 1) into nomSchema;
		if not exists(select 1 from information_schema.schemata where schema_name = nomSchema) then
			execute format('create schema %s; grant usage on schema %s to public;', nomSchema, nomSchema);
			-- Et nous sommes supposés tourner en tant que super-utilisateur, qui finira donc proprio du schéma avec aucun droit pour les autres.
		else
			-- À FAIRE: s'assurer qu'il est bien configuré en droits, ou au moins péter une exception s'il ne nous appartient pas.
		end if;
		return nomSchema;
	end;
$$;

create or replace function _sudo_verifierTable(nomTable text, colonnes text) returns void language plpgsql as
$$
	begin
		if not exists(select 1 from information_schema.tables where table_schema||'.'||table_name = nomTable) then
			execute format('create table %s (%s)', nomTable, colonnes);
		else
			-- À FAIRE: s'assurer que les droits sont correctement configurés.
			-- À FAIRE: s'assurer que les colonnes souhaitées sont bien là. Ou laisser péter à l'exécution?
		end if;
	end;
$$;

select _sudo_installer(SUDOERS, SUDO_TRACES);

drop function _sudo_verifierTable(text, text);
drop function _sudo_verifierSchemaDe(text);
drop function _sudo_installer(text, text);
drop function _sudo_def(text, text, text, text);

commit;
