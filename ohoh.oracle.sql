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
#define OHOH_COLS     nouveau, null, sysdate
#define OHOH_COLS_DEF cast(null as DEDE_ID_TYPE_DIM) rempl_par, cast(null as varchar2(255)) rempl_cause, sysdate rempl_date
#endif

-- À FAIRE: la version avec OHOH_MAJ pour mettre à jour une entrée récente plutôt que d'en rajouter une. Cf. ohoh.pg.sql.

create or replace function LOCAL(ohoh)(nomTable varchar2, ancien DEDE_ID_TYPE, nouveau DEDE_ID_TYPE, commentaire varchar2)
	return integer
as
	r integer;
	-- https://stackoverflow.com/a/38610104
	table_does_not_exist exception;
	pragma exception_init(table_does_not_exist, -942);
	-- https://stackoverflow.com/a/8729553
	pragma autonomous_transaction;
	begin
		select LOCAL(ohoh_)(nomTable, ancien, nouveau, commentaire) into r from dual;
		return r;
	exception when table_does_not_exist then
		execute immediate
		'
			create table '||nomTable||'OHOH_SUFFIXE as
				select OHOH_COLS_DEF, t.* from '||nomTable||' t where rownum < 0
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
	-- https://stackoverflow.com/a/8729553
	-- Bien penser au commit (sans quoi ORA-06519: transaction autonome active détectée et annulée).
	pragma autonomous_transaction;
	begin
		execute immediate
		'
			insert into '||nomTable||'OHOH_SUFFIXE
				select '||replace('OHOH_COLS', 'nouveau', ''''||nouveau||'''')||', t.* from '||nomTable||' t where DEDE_ID in ('''||ancien||''')
		';
		commit;
		return 1;
	end;
/
