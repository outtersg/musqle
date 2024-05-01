#if 0
-- https://www.nazmulhuda.info/how-to-generate-md5-value-in-oracle
-- Mais pas facile d'avoir une comparaison de chaîne > 4000 caractères:
-- https://asktom.oracle.com/ords/asktom.search?tag=equality-predicate-on-clob
-- https://dbaora.com/ora_hash-and-clob-problem/
-- ora_hash est cumulatif (le dernier paramètre permet de balader l'état d'une itération à l'autre), mais sur 32 bit,
-- et dbms_obfuscation_toolkit.md5 est en 128 bits mais non cumulatif.
-- On bidouille donc quelque chose en utilisant en matière d'accu l'empreinte de la précédente itération collée façon piment crypto.
#endif
create or replace function crc_lob(chaine in clob) return varchar2 as
	empreinte varchar2(32);
	pos integer;
	paquet integer := 32000;
begin
	pos := 1 + floor((length(chaine) - 1) / paquet) * paquet;
	empreinte := '';
	while pos > 0 loop
		dbms_obfuscation_toolkit.md5(input => utl_raw.cast_to_raw(empreinte||substr(chaine, pos, paquet)), checksum => empreinte);
		pos := pos - paquet;
	end loop;
	return empreinte;
end;
