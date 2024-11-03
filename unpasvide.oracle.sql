create or replace function unpasvide(nom_table varchar2) return int as
	r int;
begin
	execute immediate 'select 1 from '||nom_table||' where rownum < 2' into r;
	return r;
end;
