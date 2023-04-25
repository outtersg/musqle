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

#if defined(DEDE_SCHEMA)
#define LOCAL(t) DEDE_SCHEMA.t
#else
#define LOCAL(t) t
#endif

#if not defined(DEDE_ID_TYPE)
#define DEDE_ID_TYPE integer
#endif
#if not defined(DEDE_ID_TYPE_DIM)
#define DEDE_ID_TYPE_DIM DEDE_ID_TYPE
#endif

#if not defined(DEDE_ID)
#define DEDE_ID id
#endif

#if not defined(OHOH_SUFFIXE)
#define OHOH_SUFFIXE _poubelle
#endif

#if defined(OHOH_SUFFIXE) and not defined(OHOH_COLS)
#define OHOH_COLS     nouveau, commentaire, sysdate
#define OHOH_COLS_DEF cast(null as DEDE_ID_TYPE_DIM) rempl_par, cast(null as varchar2(255)) rempl_cause, sysdate rempl_date
#endif

-- À FAIRE: la version avec OHOH_MAJ pour mettre à jour une entrée récente plutôt que d'en rajouter une. Cf. ohoh.pg.sql.
-- À FAIRE: émettre des fonctions précompilées pour chaque table.
--          Problème: comment détecter que la table a changé si c'est une version énumérant les colonnes (alors que le t.* nous assure que notre fonction est toujours alignée sur la table réelle)?

create or replace function LOCAL(ohoh)(nomTable varchar2, ancien DEDE_ID_TYPE, nouveau DEDE_ID_TYPE, commentaire varchar2)
	return integer
as
	r integer;
	colonnes clob;
	-- https://stackoverflow.com/a/38610104
	table_does_not_exist exception;
	pragma exception_init(table_does_not_exist, -942);
	-- https://stackoverflow.com/a/8729553
	pragma autonomous_transaction;
	begin
		select LOCAL(ohoh_)(nomTable, ancien, nouveau, commentaire) into r from dual;
		return r;
	exception when table_does_not_exist then
		select LOCAL(ohoh_colonnes(nomTable, null)) into colonnes from dual;
		execute immediate
		'
			create table '||nomTable||'OHOH_SUFFIXE as
				select OHOH_COLS_DEF, '||colonnes||' from '||nomTable||' t where rownum < 0
		'
		;
		execute immediate 'create index '||nomTable||'OHOH_SUFFIXE'||'_id_i on '||nomTable||'OHOH_SUFFIXE(DEDE_ID)';
		-- À FAIRE: les deux suivants en fonction du contenu de OHOH_COLS_DEF
		execute immediate 'create index '||nomTable||'OHOH_SUFFIXE'||'_par_i on '||nomTable||'OHOH_SUFFIXE(rempl_par)';
		execute immediate 'create index '||nomTable||'OHOH_SUFFIXE'||'_cause_i on '||nomTable||'OHOH_SUFFIXE(rempl_cause)';
		commit;
		select LOCAL(ohoh_)(nomTable, ancien, nouveau, commentaire) into r from dual;
		return r;
	end;
/

create or replace function LOCAL(ohoh_)(nomTable in varchar2, ancien in DEDE_ID_TYPE, nouveau in DEDE_ID_TYPE, commentaire in varchar2)
	return integer
as
	colonnes clob;
	-- https://stackoverflow.com/a/8729553
	-- Bien penser au commit (sans quoi ORA-06519: transaction autonome active détectée et annulée).
	pragma autonomous_transaction;
	begin
		select LOCAL(ohoh_colonnes(nomTable, null)) into colonnes from dual;
		execute immediate
		'
			insert into '||nomTable||'OHOH_SUFFIXE
				select '||replace(replace('OHOH_COLS', 'nouveau', ''''||nouveau||''''), 'commentaire', case when commentaire is null then null else ''''||replace(commentaire, '''', '''''')||'''' end)||', '||colonnes||' from '||nomTable||' t where DEDE_ID in ('''||ancien||''')
		';
		commit;
		return 1;
	end;
/

-- Perfs:
-- 3,1 s lower(table_name) = lower(x) or lower(owner||'.'||table_name) = lower(x) -- Adaptabilité maximale.
-- 2,5 s sans les lower (si on écrit déjà dans la bonne casse)
-- 0,3 s juste sur le nom de table.
-- Bref pour une question de perfs, merci d'appeler ohoh_colonnes avec le nom de table sans le schéma!

#define TABLE_NOMMÉE(x) (lower(table_name) = lower(x) or lower(owner||'.'||table_name) = lower(x))
#define TABLE_NOMMÉE(x) (table_name = upper(x) and (schema is null or owner = upper(schema)))

-- https://stackoverflow.com/questions/29116396/workaround-for-ora-00997-illegal-use-of-long-datatype
create or replace function LOCAL(ohoh_colonnes)(nomTable in varchar2, schema in varchar2)
	return clob
as
	colonnes clob;
	versionLongue integer;
	begin
		select
			case when exists (select 1 from all_tab_columns where TABLE_NOMMÉE(nomTable) and data_type in ('LONG')) then 1
			else 0
			end
		into versionLongue from dual;
		if versionLongue = 0 then return 't.*'; end if;
		
		-- listagg est soumis à la limite des 4000 caractères, même sur du clob pour produire du clob (list_agg(to_clob(…)), grrr…
		-- https://stackoverflow.com/questions/74037009/how-i-can-use-listagg-with-clob
		-- On doit donc boucler sur des paquets de moins de 4000 caractères et agréger nous-mêmes, lorsque le schéma est vraiment, vraiment chargé.
		-- À FAIRE: version mi-longue si sum(length(column_name)) < 3000 (de la marge pour inclure les to_lob()).
		colonnes := '';
		for colonne in
		(
			select
				listagg(case when data_type = 'LONG' then 'to_lob('||column_name||') '||column_name else column_name end, ',') within group (order by column_id) trad
			from all_tab_columns
			where TABLE_NOMMÉE(nomTable)
			group by floor(column_id / 100)
		)
		loop
			if length(colonnes) > 0 then colonnes := colonnes||','; end if;
			colonnes := colonnes||colonne.trad;
		end loop;
		return colonnes;
	end;
/
