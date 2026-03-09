whenever sqlerror exit failure rollback
set serveroutput on

declare
  v_schema_username varchar2(128) := :schema_username;
  n number;
begin
  select count(*) into n from all_users where username = upper(v_schema_username);
  if n = 1 then
    execute immediate 'drop user ' || lower(v_schema_username) || ' cascade';
    dbms_output.put_line('Dropped user ' || lower(v_schema_username));
  else
    dbms_output.put_line('User ' || lower(v_schema_username) || ' not present; skipping');
  end if;
end;
/

prompt
prompt Done. Verify:
prompt   select username from all_users where username = upper('your_schema_name');
