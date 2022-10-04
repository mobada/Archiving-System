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
--------------------------------------------------------
--  DDL for Sequence DWH_ARCH_DATA_ACTION_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "ARCHDATA"."DWH_ARCH_DATA_ACTION_SEQ"  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE  NOKEEP  NOSCALE  GLOBAL ;
--------------------------------------------------------
--  DDL for Sequence DWH_ARCH_DATA_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "ARCHDATA"."DWH_ARCH_DATA_SEQ"  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 536341 CACHE 20 NOORDER  NOCYCLE  NOKEEP  NOSCALE  GLOBAL ;
--------------------------------------------------------
--  DDL for Sequence DWH_ARCH_DATA_SETTING_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "ARCHDATA"."DWH_ARCH_DATA_SETTING_SEQ"  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 122 CACHE 20 NOORDER  NOCYCLE  NOKEEP  NOSCALE  GLOBAL ;

--------------------------------------------------------
--  DDL for Table DWH_ARCH_DATA
--------------------------------------------------------

  CREATE TABLE "ARCHDATA"."DWH_ARCH_DATA" ("ARCH_ID" NUMBER, "DOC_TYPE" VARCHAR2(100 BYTE) , "OWNER" VARCHAR2(100 BYTE) , "TABLE_NAME" VARCHAR2(100 BYTE) , "TABLE_PK_ID" NUMBER, "ARCH_DATE" TIMESTAMP (6), "STATUS" VARCHAR2(100 BYTE) , "QUERY" CLOB , "JSON_DATA" CLOB , "ACTION_USER" NUMBER, "APP_ID" NUMBER, "PAGE_NO" NUMBER, "ACTION_DATE" TIMESTAMP (6), "CONDITION" VARCHAR2(4000 BYTE) , "ROW_STATUS" VARCHAR2(100 BYTE) , "ACTION_TYPE" VARCHAR2(100 BYTE) , "ACTION_ID" NUMBER, "CURRENT_SQL" CLOB , "ERROR_MESSAGE" CLOB , "OPERATION" VARCHAR2(100 BYTE) , "LOCAL_TRANSACTION_ID" VARCHAR2(100 BYTE) )  ;
--------------------------------------------------------
--  DDL for Table DWH_ARCH_DATA_SETTING
--------------------------------------------------------

  CREATE TABLE "ARCHDATA"."DWH_ARCH_DATA_SETTING" ("SETT_ID" NUMBER, "DOC_TYPE_ID" VARCHAR2(100 BYTE) , "OWNER" VARCHAR2(100 BYTE) , "TABLE_NAME" VARCHAR2(100 BYTE) , "PK_COL_NAME" VARCHAR2(100 BYTE) , "CONDITION" VARCHAR2(4000 BYTE) , "SELECT_TYPE" VARCHAR2(100 BYTE) , "QYERY" CLOB , "ACTION_USER" NUMBER, "ACTION_DATE" TIMESTAMP (6), "SEQ" NUMBER, "ROW_PERFIX" VARCHAR2(100 BYTE) )  ;
--------------------------------------------------------
--  DDL for Table SM_DOC_TYPE
--------------------------------------------------------

  CREATE TABLE "ARCHDATA"."SM_DOC_TYPE" ("DOC_TYPE_ID" VARCHAR2(100 BYTE) , "DOC_DESCRIPTION" VARCHAR2(200 BYTE) ,active number default 1)  ;
--------------------------------------------------------
--  DDL for Trigger DWH_ARCH_DATA_TRG
--------------------------------------------------------

  CREATE OR REPLACE  TRIGGER "ARCHDATA"."DWH_ARCH_DATA_TRG" 
BEFORE INSERT OR UPDATE ON archdata.DWH_ARCH_DATA 
FOR EACH ROW 
BEGIN
  if inserting then
    if :new.arch_id is null then
        :new.arch_id := DWH_ARCH_DATA_seq.nextval;
    end if;
  end if;

:new.DOC_TYPE:=upper(:new.DOC_TYPE);
:new.OWNER:=upper(:new.OWNER);
:new.TABLE_NAME:=upper(:new.TABLE_NAME);
:new.TABLE_PK_ID:=upper(:new.TABLE_PK_ID);
:new.STATUS:=upper(:new.STATUS);
:new.CONDITION:=upper(:new.CONDITION);
:new.ROW_STATUS:=upper(:new.ROW_STATUS);


:new.app_id := V('APP_ID');
:new.page_no := V('APP_PAGE_ID');
:new.action_date := systimestamp;
END;
/
ALTER TRIGGER "ARCHDATA"."DWH_ARCH_DATA_TRG" ENABLE
--------------------------------------------------------
--  DDL for Trigger DWH_ARCH_DATA_SETTING_TRG
--------------------------------------------------------

  CREATE OR REPLACE  TRIGGER "ARCHDATA"."DWH_ARCH_DATA_SETTING_TRG" 
BEFORE INSERT or update or delete ON ARCHDATA.DWH_ARCH_DATA_SETTING
FOR EACH ROW 
BEGIN
if inserting then
  if :new.sett_id is null then
    :new.sett_id := DWH_ARCH_DATA_SETTING_seq.nextval;
end if;
if :new.DOC_TYPE_ID is null then
  :new.DOC_TYPE_ID := V('P3_DOC_TYPE_ID');
end if;
end if;

if inserting or updating then
  :new.action_date := systimestamp;

end if;

if deleting then
null;
end if;

END;
/
ALTER TRIGGER "ARCHDATA"."DWH_ARCH_DATA_SETTING_TRG" ENABLE;
--------------------------------------------------------
--  Constraints for Table DWH_ARCH_DATA
--------------------------------------------------------



--------------------------------------------------------
--  Constraints for Table DWH_ARCH_DATA_SETTING
--------------------------------------------------------


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

-----------------------ARCH_DATA_PKG ---------------------------
create or replace PACKAGE          ARCHDATA.ARCH_DATA_PKG AS 

  /* Middle code to handle data archiving logic and process flow
   Mohammed Obada 06-2022*/ 

type g_jason_data_rec is record (data_c clob); 
type g_jason_data is table of g_jason_data_rec;

type g_strg_rec is record (value_c varchar2(4000));
type g_strg_tab is table of g_strg_rec;

/*Function to get table primary key column name
Mohammed Obada 06-2022*/
function get_table_pk_column(p_owner varchar2,p_table_name varchar2) return varchar2;

/*Function to return string of jason object tables columns
Mohammed Obada 06-2022*/
function get_tab_jason_strng(p_owner varchar2,p_table_name varchar2) return varchar2;

/*Function returns jason object with data of a given table name
Mohammed Obada 06-2022*/
function get_table_jason_data(p_owner varchar2,p_table_name varchar2,p_table_pk_id number default null,p_condition varchar2 default null) return g_jason_data PIPELINED;

/*Functio to split strig into table
Mobada 06-2022*/
function split_strg_into_table(p_string varchar2,p_split varchar2 default ',') return g_strg_tab PIPELINED;

/*Function gets g_query variable
Mohammed Obada 06-2022*/
function get_g_query return varchar2;


/*Procedure to create audit triggger on given table
Mobada 06-2022*/
Procedure create_audit_trigger(p_doc_type_id varchar2,p_owner varchar2,p_table_name varchar2,p_pk_col varchar2);

/*Procedure to drop audit trigger
Mohammed Obada 06-2022*/
Procedure drop_audit_trigger(p_owner varchar2,p_table_name varchar2);

/* Procedure to create job to execeute json in DWH archiving table
To avoid execution of it many times durring the main transaction
Mohammed Obada 06-2022*/
Procedure exec_json_job(p_local_trans_id varchar2);

/* Function to execute row sql and returns DONE if all rows of given tansaction id is done
Mohammed Obada 06-2022*/
function exec_arch_row(p_trans_id varchar2) return varchar2;

/* Flush stoped and disabled jobs
Mohammed Obada 06-2022*/
procedure flush_arch_jobs; 

/* Function to archive document type based on its setting
Mobada 19-04-2022
Modified by Mobada 15-05-2022 to add p_table_name_or_row_perfix*/
function arch_doc_type(p_doc_type_id varchar2,p_pk_id varchar2,p_operation varchar2 default null,p_table_name_or_row_perfix varchar2 default null) return number;



END ARCH_DATA_PKG;
/
create or replace PACKAGE BODY           ARCHDATA.ARCH_DATA_PKG AS

 /* Middle code to handle data archiving logic and process flow
   Mohammed Obada 06-2022*/ 

v_query varchar2(32000);

/*Function to get table primary key column name 
Mohammed Obada 06-2022*/
function get_table_pk_column(p_owner varchar2,p_table_name varchar2) return varchar2
is 

cursor cons
is 
select cc.column_name
from dba_constraints c,dba_cons_columns cc
where cc.owner = c.owner
and cc.table_name = c.table_name
and cc.constraint_name = c.constraint_name
and c.owner=p_owner
and c.table_name = p_table_name
and c.constraint_type = 'P';

l_col varchar2(4000);
begin
open cons;
fetch cons into l_col;
close cons;

return l_col;
end get_table_pk_column;

/*Function to return string of jason object tables columns
Mohammed Obada 06-2022*/
function get_tab_jason_strng(p_owner varchar2,p_table_name varchar2) return varchar2
is 
cursor cols
is 
select * 
from dba_tab_columns 
where owner = p_owner
and table_name = p_table_name;  

l_col varchar2(32000) := '('; 
begin
for c in cols loop
l_col := l_col||','''||c.column_name||''' value '||c.column_name;
end loop;
l_col := replace(l_col,'(,','(');
l_col := 'json_object'||l_col||')';

return l_col;
end get_tab_jason_strng;

/*Function returns jason object with data of a given table name
Mohammed Obada 06-2022*/
function get_table_jason_data(p_owner varchar2,p_table_name varchar2,p_table_pk_id number default null,p_condition varchar2 default null) return g_jason_data PIPELINED
is
l_query varchar2(32000);
l_where varchar2(32000);
l_cur sys_refcursor;
l_jason_data_rec g_jason_data_rec;
l_jason_data g_jason_data;
l_table_pk_col varchar2(100);
begin

if p_condition is not null then
l_where := p_condition;
end if;

if p_table_pk_id is not null then
l_table_pk_col := get_table_pk_column(p_owner,p_table_name);
if l_table_pk_col is not null then
l_where := case when l_where is not null then l_where||' and ' else '' end ||l_table_pk_col||' = '||p_table_pk_id;
end if;
end if;

if l_where is not null then
l_where := ' where '||l_where;
end if;

--l_query := 'select '||get_tab_jason_strng(p_owner,p_table_name)||' from '||p_owner||'.'||p_table_name||l_where;
l_query := 'select json_object(*) from '||p_owner||'.'||p_table_name||l_where;
v_query := l_query;
open l_cur for l_query;
fetch l_cur BULK COLLECT INTO l_jason_data;
close l_cur;

for i in 1..l_jason_data.count loop
pipe row (l_jason_data(i));
end loop;
return;
end get_table_jason_data;

/*Functio to split strig into table
Mohammed Obada 06-2022*/
function split_strg_into_table(p_string varchar2,p_split varchar2 default ',') return g_strg_tab PIPELINED
is
l_split varchar2(100) := p_split;
l_strg_tab g_strg_tab;
cursor strgs
is
select regexp_substr(p_string,'[^'||l_split||']+', 1, level) 
   from dual 
   connect BY regexp_substr(p_string, '[^'||l_split||']+', 1,level) is not null;
begin
 open strgs;
 fetch strgs bulk collect into l_strg_tab;
 close strgs;

 for i in 1..l_strg_tab.count loop
pipe row (l_strg_tab(i));
end loop;

 return ;
end split_strg_into_table;

/*Function gets g_query variable
Mohammed Obada 06-2022*/
function get_g_query return varchar2
is
begin
return v_query;
end get_g_query;

/*Function to insert document data as a json in DWH_ARCH_DATA
returns arch_id
Mohammed Obada 06-2022*/
function insert_doc_data(p_doc_type_id varchar2,p_owner varchar2,p_table_name varchar2,p_table_pk_id number,p_condition varchar2 default null,p_doc_staus varchar2 default null) return number
is
l_query clob;
l_json clob;
l_arch_id number;


cursor json_data
is
select *
from ARCH_DATA_PKG.get_table_jason_data(p_owner,p_table_name,p_table_pk_id,p_condition);

begin

open json_data;
fetch json_data into l_json ;
close json_data; 
 
l_query := get_g_query;
l_arch_id := DWH_ARCH_DATA_seq.nextval;

update DWH_ARCH_DATA
set row_status = 'HISTORY'
where  condition = p_condition
and table_pk_id = p_table_pk_id
and owner = p_owner
and table_name = p_table_name
and doc_type = p_doc_type_id;


insert into DWH_ARCH_DATA  
(arch_id,
DOC_TYPE,
OWNER,
TABLE_NAME,
TABLE_PK_ID,
STATUS,
QUERY,
JSON_DATA,
condition)
values
(l_arch_id,
p_doc_type_id,
p_owner,
p_table_name,
p_table_pk_id,
p_doc_staus,
l_query,
l_json,
p_condition
);

return l_arch_id;
end insert_doc_data;


/* Procedure to create audit triggger on given table
Mohammed Obada 06-2022*/
Procedure create_audit_trigger(p_doc_type_id varchar2,p_owner varchar2,p_table_name varchar2,p_pk_col varchar2)
is

l_sql varchar2(32000);
begin

l_sql := 'CREATE OR REPLACE TRIGGER '||p_owner||'.'||p_table_name||'_AU '||
            'for insert or update or delete on '||p_owner||'.'||p_table_name||' compound trigger 
            l_return number;
    l_operation varchar2(100);
    type ids_t is table of number
    index by binary_integer;
    l_ids ids_t;
    before each row is
    begin  
    if deleting then
    l_ids(l_ids.count+1) := :old.'||p_pk_col||';
    l_operation := ''DELETING'';
    end if;
    end before each row;

    after each row is
    begin
    if not deleting then
    l_ids(l_ids.count+1) := :new.'||p_pk_col||';
    l_operation := CASE WHEN UPDATING THEN ''UPDATING''
                ELSE ''INSERTING'' END;
    end if;
    end after each row;

    after statement is
    BEGIN
    for i in 1..l_ids.count loop
    l_return := ARCH_DATA_PKG.arch_doc_type('||''''||p_doc_type_id||''''||',l_ids(i),l_operation);
    end loop;
    END after statement;
    end;';

execute immediate l_sql;
end create_audit_trigger;

/*Procedure to drop audit trigger
Mohammed Obada 06-2022*/
Procedure drop_audit_trigger(p_owner varchar2,p_table_name varchar2)
is
l_sql varchar2(32000);
l_trg_name varchar(1000);

cursor trgs 
is
select owner||'.'||trigger_name
from dba_triggers 
where owner = p_owner
and trigger_name = p_table_name||'_AU';

begin
open trgs;
fetch trgs into l_trg_name;
close trgs;

if l_trg_name is not null then
l_sql := 'drop trigger '||p_owner||'.'||p_table_name||'_AU';
execute immediate l_sql;
end if;

end drop_audit_trigger;

/* Procedure to create job to execeute json in DWH archiving table
To avoid execution of it many times durring the main transaction
Mohammed Obada 06-2022*/
Procedure exec_json_job(p_local_trans_id varchar2)
is
PRAGMA AUTONOMOUS_TRANSACTION;

l_action varchar2(32000);
l_ora_job_name varchar2(1000) := 'ARCH_'||replace(p_local_trans_id,'.','_');
l_job number := 0;
cursor jobs
is
SELECT 1 
FROM user_scheduler_jobs
WHERE job_name = 'ARCH_'||replace(p_local_trans_id,'.','_');

begin
open jobs;
fetch jobs into l_job;
close jobs;
if nvl(l_job,0) = 0 then
l_action := 'declare 
            l_done varchar2(100);
            begin

            l_done := ARCH_DATA_PKG.exec_arch_row('||''''||p_local_trans_id||''''||');
            dbms_scheduler.enable('||''''||l_ora_job_name||''''||'); 
            if l_done = ''DONE'' then
             dbms_scheduler.drop_job('||''''||l_ora_job_name||''''||',force => TRUE ); 
             end if;
             end;';
dbms_scheduler.create_job(job_name =>l_ora_job_name,
                                 job_type => 'PLSQL_BLOCK',
                                 start_date => NULL,
            repeat_interval => 'FREQ=SECONDLY;BYDAY=MON,TUE,WED,THU,FRI,SAT,SUN',
            end_date => NULL,
                                 job_action => l_action);
dbms_scheduler.enable(l_ora_job_name);

end if;

end exec_json_job;




/* Function to execute row sql and returns DONE if all rows of given tansaction id is done
Mohammed Obada 06-2022*/
function exec_arch_row(p_trans_id varchar2) return varchar2
is

cursor archs
is
select *
from DWH_ARCH_DATA
where json_data is null
and row_status = 'CURRENT'
and (local_transaction_id = p_trans_id or p_trans_id is null)
order by 1;

l_json clob;
l_err_code number;
l_err_msg varchar2(32000);
l_done varchar2(10) := 'NO';
begin

for c in archs loop
l_json := null;
l_err_code := null;
l_err_msg := null;
l_done := 'DONE';
begin

execute immediate c.query into l_json ;

update DWH_ARCH_DATA
set json_data = l_json
where arch_id = c.arch_id;



exception 
when others then
l_err_code := SQLCODE;
l_err_msg := SQLERRM;

update DWH_ARCH_DATA
set row_status = 'ERROR',error_message=l_err_code||' -- '||l_err_msg
where arch_id = c.arch_id;
end;
end loop;
return l_done;
end exec_arch_row;

/* Flush stoped and disabled jobs
Mohammed Obada 06-2022*/
procedure flush_arch_jobs
is
cursor jobs
is
select *
from dba_scheduler_jobs 
where job_name like 'ARCH_%'
and ((enabled != 'FALSE' and trunc(start_date) < trunc(sysdate)) or enabled = 'FALSE');
begin
for j in jobs loop
dbms_scheduler.drop_job(j.job_name,force => TRUE ); 
end loop;
end flush_arch_jobs;

/*Function to archive document type based on its setting
Mohammed Obada 06-2022
Modified by Mobada 15-07-2022 to add p_table_name_or_row_perfix
Modified by MObada 08-08-2022 to add create job technique*/
function arch_doc_type(p_doc_type_id varchar2,p_pk_id varchar2,p_operation varchar2 default null,p_table_name_or_row_perfix varchar2 default null) return number
is
cursor doc_setting
is
select *
from DWH_ARCH_DATA_SETTING
where (table_name = p_table_name_or_row_perfix or row_perfix = p_table_name_or_row_perfix
        or p_table_name_or_row_perfix is null)
and doc_type_id = p_doc_type_id
order by seq;

l_action_id number;
l_arch_id number;
l_sql varchar2(32000);
l_json clob;
l_action varchar2(4000);
l_curr_sql clob;
l_err_code number;
l_err_msg varchar2(32000);
l_row_staus varchar2(100);
l_using_clause varchar2(1000);
l_local_trans_id varchar2(1000);
l_row_exist number;

l_pk_id varchar2(1000);
l_pk_id_value varchar2(1000);
pk_is_null EXCEPTION;

cursor row_exist(p_tab_name varchar2,p_owner varchar2,p_cond varchar2)
is
select 1
from DWH_ARCH_DATA 
where  
 nvl(condition,'NON') = nvl(p_cond,'NON')
and table_pk_id = l_pk_id_value
and owner = p_owner
and table_name = p_tab_name
and LOCAL_TRANSACTION_ID = l_local_trans_id;
--PRAGMA AUTONOMOUS_TRANSACTION;
begin
l_action_id := DWH_ARCH_DATA_ACTION_SEQ.nextval;
l_action := sys_context('userenv','ACTION') ;
l_local_trans_id := DBMS_TRANSACTION.LOCAL_TRANSACTION_ID;
l_curr_sql := sys_context('userenv','current_sql')|| sys_context('userenv','current_sql1')||
                sys_context('userenv','current_sql2')||sys_context('userenv','current_sql3')||
                sys_context('userenv','current_sql5')||sys_context('userenv','current_sql5')||
                sys_context('userenv','current_sql7')||sys_context('userenv','current_sql7');

exec_json_job(l_local_trans_id); 
for d in doc_setting loop

l_arch_id := null;
l_row_exist := 0;
l_sql := null;
l_json := null;
l_err_code := null;
l_err_msg := null;
l_pk_id := p_pk_id;


begin

if p_pk_id is null then

l_err_code := '200005';
l_err_msg := 'PK can not be null';
raise pk_is_null;
end if;

l_pk_id := case
            when d.PK_COL_NAME like '%:1%' then replace(d.PK_COL_NAME,':1',p_pk_id)
            else
            p_pk_id
            end;

if d.PK_COL_NAME like '%:1%' then 
execute immediate 'select '||l_pk_id||' from dual' into l_pk_id_value;
else
l_pk_id_value := l_pk_id;
end if;


l_arch_id := DWH_ARCH_DATA_seq.nextval;
l_row_staus := case p_operation
            WHEN 'DELETING' THEN 'HISTORY' 
            WHEN 'UPDATING' THEN 'CURRENT'
            WHEN 'INSERTING' THEN 'CURRENT'
            ELSE
            'CURRENT'
            END;
if d.SELECT_TYPE = 'QUERY' then
l_sql := 'select json_object(* RETURNING CLOB) from 
          (select '||''''||d.table_name||''''||' as TABLE_NAME,JSON_ARRAYAGG(json_object(* RETURNING CLOB) RETURNING CLOB) as TABLE_DATA
          from ('||replace(d.QYERY,':1',p_pk_id)||'))';
else
l_sql := 'select json_object(* RETURNING CLOB) from 
          (select '||''''||d.table_name||''''||' as TABLE_NAME,JSON_ARRAYAGG(json_object(* RETURNING CLOB) RETURNING CLOB) as TABLE_DATA
          from '||d.table_name||
          case 
          when d.PK_COL_NAME is not null then
          ' where '||d.PK_COL_NAME||'='|| l_pk_id
          else '' 
          end;
l_sql := l_sql||' '||
         case
         when d.PK_COL_NAME is not null and d.CONDITION is not null then ' and ' 
         when d.PK_COL_NAME is null and d.CONDITION is not null then ' where '
         else '' end||
        case
        when d.CONDITION is null then
        ' )'
        when d.CONDITION like ('%:1%') then
        '  '||replace(d.CONDITION,':1',p_pk_id)||' )'
        else
        '  '||d.CONDITION||' )'
        end;
end if;


open row_exist(nvl(d.table_name,d.row_perfix),d.owner,nvl(d.condition,'NON'));
fetch row_exist into l_row_exist;
close row_exist;
--execute immediate l_sql into l_json ;

if nvl(l_row_exist,0) = 0 then
update DWH_ARCH_DATA
set row_status = 'HISTORY'
where  /*LOCAL_TRANSACTION_ID != l_local_trans_id
and*/ nvl(condition,'NON') = nvl(d.condition,'NON')
and table_pk_id = l_pk_id_value
and owner = d.owner
--and table_name = nvl(d.table_name,d.row_perfix);
and doc_type = p_doc_type_id;

insert into DWH_ARCH_DATA  
(arch_id,
DOC_TYPE,
OWNER,
TABLE_NAME,
TABLE_PK_ID,
STATUS,
QUERY,
JSON_DATA,
condition,
action_type,
action_id,
current_sql,
row_status,
operation,
LOCAL_TRANSACTION_ID)

values

(l_arch_id,
p_doc_type_id,
d.owner,
nvl(d.table_name,d.row_perfix),
l_pk_id_value,
'',
l_sql,
l_json,
d.condition,
l_action,
l_action_id,
l_curr_sql,
l_row_staus,
p_operation,
l_local_trans_id 
);
end if;
exception 
when others then
l_err_code := SQLCODE;
l_err_msg := SQLERRM;


insert into DWH_ARCH_DATA  
(arch_id,
DOC_TYPE,
OWNER,
TABLE_NAME,
TABLE_PK_ID,
STATUS,
QUERY,
JSON_DATA,
condition,
action_type,
action_id,
current_sql,
error_message,
row_status,
operation,
LOCAL_TRANSACTION_ID)

values

(l_arch_id,
p_doc_type_id,
d.owner,
nvl(d.table_name,d.row_perfix),
l_pk_id_value,
'',
l_sql,
'',
d.condition,
l_action,
l_action_id,
l_curr_sql,
l_err_code||' -- '||l_err_msg,
'ERROR',
p_operation,
l_local_trans_id
);

end; 
end loop;

delete DWH_ARCH_DATA
where LOCAL_TRANSACTION_ID = DBMS_TRANSACTION.LOCAL_TRANSACTION_ID
and row_status = 'HISTORY';

return l_action_id;

end arch_doc_type;


END ARCH_DATA_PKG;
/
