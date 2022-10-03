create  user archdata identified by Au61t#123456 ;
/
grant dba,resource,connect to archdata;
/
grant select on dba_constraints to archdata;
/
grant select on dba_cons_columns to archdata;
/
grant select on dba_tab_columns to archdata;
/
grant select on dba_triggers to archdata;
/
grant select on dba_scheduler_jobs to archdata;
/
grant create any trigger to archdata;
/
grant create any job to archdata;
/
declare
cursor tabs
IS
select owner,table_name
FROM dba_tables
where upper(owner) = upper('&table_owner_name');
cursor vws
IS
select owner,view_name
FROM dba_views
where upper(owner) = upper('&view_owner_name');
BEGIN
--Grant tables
for t in tabs LOOP
execute immediate 'grant select on '||t.owner||'.'||t.table_name||' to archdata with grant option';
end loop;
--Grant views
for v in vws LOOP
begin
execute immediate 'grant select on '||v.owner||'.'||v.view_name||' to archdata';
exception
when others then
null;
end;
end loop;
end;
/
