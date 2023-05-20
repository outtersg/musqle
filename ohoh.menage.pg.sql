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

-- Supprime les entrées redondantes d'une table OHOH.

#if not defined(OHOH_SUFFIXE)
#define OHOH_SUFFIXE _poubelle
#endif

#if not defined(OHOH_CLE)
#define OHOH_CLE id, rempl_cause
#endif
#if not defined(OHOH_COLQ)
#define OHOH_COLQ rempl_date
#endif

#if not defined(TABLE)
Erreur: vous devez définir la variable TABLE dont purger la OHOH_SUFFIXE;
#endif

#set TABLEAU concat(TABLE, OHOH_SUFFIXE)

drop table if exists pg_temp.t_dp;
create temporary table t_dp as
	select OHOH_CLE, min(OHOH_COLQ) mind, max(OHOH_COLQ) maxd
	from TABLEAU
#if defined(FILTRE)
	where FILTRE
#endif
	group by 1, 2
	-- On garde de toute manière la première et la dernière. La question de purger ne se pose que pour celles entre.
	having count(1) > 2
;
-- Candidats à la purge (toutes les entrées sauf la première et la dernière):
drop table if exists pg_temp.t_cp;
create temporary table t_cp as
	select
	row_number() over() pos,
	OHOH_CLE, OHOH_COLQ,
	coalesce(first_value(OHOH_COLQ) over (partition by OHOH_CLE order by OHOH_COLQ rows between 1 preceding and 1 preceding), mind) ohoh_dern
	from t_dp join TABLEAU p using (OHOH_CLE)
	where p.OHOH_COLQ > mind and p.OHOH_COLQ < maxd
;
-- Les entrées qui diffèrent de leur entrée prédecesseur ne serait-ce que sur un champ, marquent une transition et sont conservées (donc sont supprimées de la liste à supprimer. Tout le monde suit?).
#set JOINTURE `select regexp_replace(regexp_replace('OHOH_CLE', '([^ ,]+)', 'a.\1 = p.\1', 'g'), ' *, *', ' and ', 'g')`
drop table if exists pg_temp.t_sp;
create temporary table t_sp as
	-- À FAIRE: un diff -q, qui sort dès qu'une différence apparaît.
	select distinct ida from qod.diff
	(
		$$
			select pos, a.*, pos, n.*
			from t_cp p
			join TABLEAU n using (OHOH_CLE, OHOH_COLQ)
			join TABLEAU a on JOINTURE and a.OHOH_COLQ = p.ohoh_dern
		$$,
		'{OHOH_COLQ}' -- Sont aussi ignorés tous les champs insignifiants paramétrés par table.
		-- À FAIRE: permettre d'exclure d'autres colonnes non significatives.
	)
;
delete from t_cp where pos in (select ida from t_sp);

-- Exécution!

#if not defined(FAIRE) or not FAIRE
begin;
#endif

with menage as
(
	delete from TABLEAU t where (OHOH_CLE, OHOH_COLQ) in (select OHOH_CLE, OHOH_COLQ from t_cp)
	returning OHOH_CLE
)
select count(1)||' photos redondantes de '||count(distinct OHOH_CLE)||' entrées distinctes supprimées' from menage;

#if not defined(FAIRE) or not FAIRE
rollback;
#include couleurs.sql
select JAUNE||'(simulation seule. Pour exécuter, relancez avec FA'||'IRE=1)'||BLANC;
#endif

drop table pg_temp.t_sp;
drop table pg_temp.t_cp;
drop table pg_temp.t_dp;
