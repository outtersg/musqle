#if 0
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

-- N.B.: ce fichier étant destiné à être inclus, l'ensemble de ses commentaires est encadré de #if 0 pour éviter de polluer l'incluant de nos notes.

--------------------------------------------------------------------------------
-- Regexp

-- Suppression des accolades imbriquées ----------------------------------------

-- Les expressions EXPR_ACCOLADES_x repèrent les accolades de premier niveau,
-- avec tout leur contenu y compris accolades imbriquées,
-- mais se gardant d'interpréter celles incluses dans des chaînes guillemetées.
-- Le x est le niveau maximal d'imbrication gérable.
-- Permet de travailler sur du JSON ou du PHP serialize.

#endif
#if !defined(EXPR_ECO)
#if defined(:pilote) and :pilote in ("pgsql")
#define EXPR_ECO 1
#else
#define EXPR_ECO 0
#endif
#endif
#define EXPR_CHAÎNE          "([^"\\]+|\\.)*"
#define EXPR_CHAÎNE_ECO      "(?:[^"\\]+|\\.)*"
#define EXPR_ACCOLADES_      {([^{}"]+|EXPR_CHAÎNE|@)*}
#define EXPR_ACCOLADES_ECO_  {(?:[^{}"]+|EXPR_CHAÎNE_ECO|@)*}
#if EXPR_ECO
#define EXPR_ACCOLADES_ EXPR_ACCOLADES_ECO_
#define EXPR_CHAÎNE EXPR_CHAÎNE_ECO
#endif
#set EXPR_ACCOLADES_1 replace(EXPR_ACCOLADES_, "|@", "")
#set EXPR_ACCOLADES_2 replace(EXPR_ACCOLADES_, "@", EXPR_ACCOLADES_1)
#set EXPR_ACCOLADES_3 replace(EXPR_ACCOLADES_, "@", EXPR_ACCOLADES_2)
#set EXPR_ACCOLADES_4 replace(EXPR_ACCOLADES_, "@", EXPR_ACCOLADES_3)
#if 0

#if 0
-- Utilisation, sur du PHP serialize:
select regexp_replace('{ezcnl;"àfairesauter";a:1234:{coucou"coucou{salu\"{t":{cnl};miam:{bla:{"coucou{salu\"{t"}}};suite{oui}', '"àfairesauter";[as]:[0-9]+:(EXPR_ACCOLADES_4|EXPR_CHAÎNE);?', '');
#endif

#if 0
-- Une version pur SQL, avec with recursive, peut se définir ainsi:
with recursive
	niveau as (select 3 nmax),
	exprs as
	(
		select '{(?:[^{}"]+|"(?:[^"\\]+|\\.)*"@)*}' e, 1 niveau
		union
		select replace(e, '@', case niveau when nmax then '' else '|'||e end), niveau + 1 from niveau, exprs
		where niveau <= nmax
	),
	expr as (select e from niveau, exprs where niveau > nmax)
select * from expr;
#endif

#endif
