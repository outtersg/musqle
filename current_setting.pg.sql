#if `select count(*) from version() where version ~ '^PostgreSQL ([0-8]\.|9\.[0-5]\.)'` == 1
-- Implémentation de current_setting à deux paramètres pour PostgreSQL < 9.6.
create or replace function current_setting_protected(name text, missing_ok boolean) returns text stable language plpgsql as
$$
begin
	begin
		return current_setting(name);
	exception when others then
		if missing_ok then
			return null;
		else
			raise;
		end if;
	end;
end;
$$;
-- On ne remplace que les occurrences avec second paramètre. Les autres continuent à passer par la fonction interne.
#define current_setting(name, missing_ok) current_setting_protected(name, missing_ok)
#endif
