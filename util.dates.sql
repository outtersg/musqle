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
-- Pseudo-jour

-- Le pseudo-jour est le jour dans le mois, mais en comptant en négatif à partir du 20.
-- Sur un mois de 31 jours, le 31 est compté comme -1 du mois suivant (et non 0), le 30 comme -2, etc.

#endif
#if defined(:pilote) && :pilote == 'oracle'
#define DERNIERDUMOIS(j) ((trunc(j, 'MM') + interval '1' month) - interval '1' day)
#else
#define DERNIERDUMOIS(j) date_trunc('month', j) + interval '1 month - 1 day'
#endif
#define PSEUDOJOUR(j) \
	extract(day from j) - case when extract(day from j) >= 20 then extract(day from DERNIERDUMOIS(j)) + 1 else 0 end
