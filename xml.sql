#define ___NOTE___ 0
#if ___NOTE___
-- Copyright (c) 2024 Guillaume Outters
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

-- N.B.: ce fichier étant destiné à être inclus, l'ensemble de ses commentaires est encadré de #if 0 pour éviter de polluer l'incluant de nos notes.

-- XAVIER ----------------------------------------------------------------------
-- XAVIER: Xml À Valider Incomplet: Étude et Reconstruction
-- BMX: Balises Manquantes Xml
-- BOXER: Balises Orphelines XML En Reconstruction
-- Sur un XML qui a été tronqué, reconstruit les balises fermantes pour en faire au moins un XML valide.
#endif
#if defined(:pilote) and :pilote == "oracle"
#define T_CLOB clob
#define PROC_LANG
#define PROC_RET   return
#define PROC_DÉBUT
#define PROC_DÉCL
#define PROC_FIN
#define POSITION(aiguille, botte) instr(botte, aiguille)
#define GSUB(source, regex, rempl) regexp_replace(source, regex, rempl)
#else
#define T_CLOB text
#define PROC_LANG  language plpgsql
#define PROC_RET   returns
#define PROC_DÉBUT $$
#define PROC_DÉCL  declare
#define PROC_FIN   $$;
#define POSITION(aiguille, botte) position(aiguille in botte)
#define GSUB(source, regex, rempl) regexp_replace(source, regex, rempl, 'g')
#endif
create or replace function xavier(xml T_CLOB) PROC_RET T_CLOB PROC_LANG as
#if PROC_DÉBUT == "$$"
$$
#endif
	PROC_DÉCL
		r T_CLOB; -- Reste.
		b0 T_CLOB; -- Bout.
		b1 T_CLOB;
		p integer;
	begin
		-- On élimine:
		-- - toute balise ouvrante non fermée, ex.: <déb
		-- - toute balise non terminée, ex.: <date>1979-1
		-- mais on garde:
		-- - un début de balise fermante, qu'on suppose suivre un contenu complet, ex.: <morceau>contenu</mo
		-- - une balise ouvrante suivie d'une sous-balise
		r := regexp_replace(xml, '(?:<[^/][^>*]*[^/]>|</[^<]*|<[^/][^>]*|<[^>]*[^/]>[^<]*)$', '');
		b0 := replace(r, chr(10), ' ');
		b0 := GSUB(b0, '<[^>]+/>|<[?][^>]*[?]>', ''); -- Les <balise/> et <?xml …?>
		b0 := GSUB(b0, '>[^<]+<', '><'); -- Le contenu textuel
		b0 := GSUB(b0, '<([^> ]+)[ ][^>]*[^/>]>', '<\1>'); -- Les attributs
		while b1 is null or length(b0) < length(b1) loop
			b1 := b0;
			b0 := GSUB(b0, '<[^/][^>]*></[^>]*>', '');
		end loop;
		b0 := '';
		loop
			p := POSITION('>', b1); -- position, locate, instr, charindex…
			exit when p = 0;
			b0 := '</'||substr(b1, 2, p - 1)||b0;
			b1 := substr(b1, p + 1);
		end loop;
		return r||b0;
	end;
PROC_FIN
#if ___NOTE___
-- { echo "#format delim \\t" ; echo "#include xml.sql" ; awk '/<\/test>/{oui=0}oui{print}/<test>/{oui=1}' < xml.sql ; } | bdd='pgsql:host=localhost port=5432 dbname=test' php ../sqleur/sql2csv.php
-- <test>
with
	t as
	(
			  select '<?xml blabla?><début><milieu><fi' e,                    '<?xml blabla?><début><milieu></milieu></début>' a
		union select '<?xml bla?><début><milieu>oui</milieu><autre>non</au',  '<?xml bla?><début><milieu>oui</milieu><autre>non</autre></début>'
		union select '<?xml bla?><début><milieu>oui</milieu><autre>no',       '<?xml bla?><début><milieu>oui</milieu></début>'
		union select '<?xml bla?><début><milieu>oui</milieu><autre><oui/><a', '<?xml bla?><début><milieu>oui</milieu><autre><oui/></autre></début>'
	),
	x as (select t.*, xavier(e) r from t)
select case when a = r then '[32moui[0m' else '[31mnon[0m' end bon, e entree, a attendu, r recu from x;
-- </test>
#endif
