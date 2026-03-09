whenever sqlerror exit failure rollback
set serveroutput on

declare
  n number;
begin
  select count(*) into n from all_users where username = 'OPEN_ARCHIVE_USER';
  if n = 1 then
    execute immediate 'drop user open_archive_user cascade';
    dbms_output.put_line('Dropped user open_archive_user');
  else
    dbms_output.put_line('User open_archive_user not present; skipping');
  end if;
end;
/

prompt
prompt Done. Verify:
prompt   select username from all_users where username in ('OPEN_ARCHIVE_USER');
