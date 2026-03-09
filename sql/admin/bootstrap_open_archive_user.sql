whenever sqlerror exit failure rollback
set verify off
set serveroutput on

declare
  v_schema_username varchar2(128) := :schema_username;
  v_schema_password varchar2(512) := :schema_password;

  function esc(p varchar2) return varchar2 is
  begin
    return replace(p, '"', '""');
  end;

  procedure ensure_user(p_username varchar2, p_password varchar2) is
    n number;
  begin
    if p_password is null or length(p_password) = 0 then
      raise_application_error(-20001, 'Password for ' || lower(p_username) || ' is empty');
    end if;

    select count(*) into n from all_users where username = upper(p_username);
    if n = 0 then
      dbms_output.put_line('Creating user ' || lower(p_username));
      execute immediate 'create user ' || lower(p_username) || ' identified by "' || esc(p_password) || '"';
    else
      dbms_output.put_line('User ' || lower(p_username) || ' already exists; skipping password change');
    end if;
  end;
begin
  ensure_user(v_schema_username, v_schema_password);
end;
/

begin
  execute immediate 'grant create session to ' || lower(:schema_username);
  execute immediate 'grant create table to ' || lower(:schema_username);
  execute immediate 'grant create view to ' || lower(:schema_username);
  execute immediate 'grant create sequence to ' || lower(:schema_username);
  execute immediate 'grant create procedure to ' || lower(:schema_username);
  execute immediate 'grant create trigger to ' || lower(:schema_username);
  execute immediate 'alter user ' || lower(:schema_username) || ' quota unlimited on data';
end;
/

prompt
prompt Done. Verify:
prompt   select username, account_status from all_users where username = upper('your_schema_name');
