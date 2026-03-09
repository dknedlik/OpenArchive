whenever sqlerror exit failure rollback
set verify off
set serveroutput on

declare
  v_open_archive_password varchar2(512) := :open_archive_password;

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
  ensure_user('OPEN_ARCHIVE_USER', v_open_archive_password);
end;
/

grant create session to open_archive_user;
grant create table to open_archive_user;
grant create view to open_archive_user;
grant create sequence to open_archive_user;
grant create procedure to open_archive_user;
grant create trigger to open_archive_user;
alter user open_archive_user quota unlimited on data;

prompt
prompt Done. Verify:
prompt   select username, account_status from all_users where username in ('OPEN_ARCHIVE_USER');
