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
create or replace function xavier(xml text) returns text language plpgsql as
$$
	declare
		r text; -- Reste.
		b0 text; -- Bout.
		b1 text;
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
		b0 := regexp_replace(b0, '<[^>]+/>|<[?][^>]*[?]>', '', 'g'); -- Les <balise/> et <?xml …?>
		b0 := regexp_replace(b0, '>[^<]+<', '><', 'g'); -- Le contenu textuel
		b0 := regexp_replace(b0, '<([^> ]+)[ ][^>]*[^/>]>', '<\1>', 'g'); -- Les attributs
		while b1 is null or length(b0) < length(b1) loop
			b1 := b0;
			b0 := regexp_replace(b0, '<[^/][^>]*></[^>]*>', '', 'g');
		end loop;
		b0 := '';
		loop
			p := position('>' in b1); -- position, locate, instr, charindex…
			exit when p = 0;
			b0 := '</'||substr(b1, 2, p - 1)||b0;
			b1 := substr(b1, p + 1);
		end loop;
		return r||b0;
	end;
$$;
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
