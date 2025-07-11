create database dataqualitypoc;
CREATE SCHEMA IF NOT EXISTS dataquality;

create table if not exists dataqualitypoc.dataquality.data_compare_check (

    process_id int,

                  check_id int,

                  active boolean,

                  check_type varchar(100),

                  chunking_rule varchar(100),

                  key_cols varchar(100),

    src_qry varchar(10000),

                  tgt_qry varchar(10000)

);

 

insert into dataqualitypoc.dataquality.data_compare_check values(11,1002,'Y','OOTB_NOTNULL','','','','dataqualitypoc.dataquality.employee.job_title');

insert into dataqualitypoc.dataquality.data_compare_check values(11,1003,'Y','OOTB_DUPCHECK','','first_name','','dataqualitypoc.dataquality.employee');

-- output of the script, created 1 row per check in above table, for a given processid, or runid

create table if not exists dataqualitypoc.dataquality.data_compare_log (

    process_id int,

                  run_id int, -- unique across all executions, generated via max+1 on this column

                  check_id int,

                  run_status varchar(100),

    numdiff int,

                  run_at timestamp,

                  run_duration float, -- duration in seconds

                  err_reason varchar(200)

);

 

-- cell level diffs

create table if not exists dataqualitypoc.dataquality.data_compare_diff (

    process_id int,

    run_id int,

                  check_id int,

                  colname varchar(40),

    pkeyval varchar(100),

                  srcval varchar(1000),

                  tgtval varchar(1000)

                 

);

 

 

-- needed only if the html report is generated, stored in a file

create table if not exists dataqualitypoc.dataquality.runstats (

    statnum         int,

    descr           varchar,

                  enabled         boolean,

                  statqry         varchar

);

 

 

insert into dataqualitypoc.dataquality.runstats values (1,'List of Active Checks', 'Y',

    'select check_id, check_type from dataqualitypoc.dataquality.data_compare_check where active=''Y'' order by check_id');

insert into dataqualitypoc.dataquality.runstats values (1,'Status of Compares & DQ Checks', 'Y','select check_id, run_status, numdiff,

    err_reason from dataqualitypoc.dataquality.data_compare_log where run_id=%runid order by check_id');

insert into dataqualitypoc.dataquality.runstats values (2,'Cell Differences found', 'Y','select check_id, colname, pkeyval, srcval,

    tgtval from dataqualitypoc.dataquality.data_compare_diff where run_id=%runid order by check_id');

-- delete from dataqualitypoc.dataquality.runstats;

 

 

CREATE TABLE if not exists dataqualitypoc.dataquality.employee (
  id int,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  gender varchar(10),
  email varchar(255),
  phone varchar(50),
  dob DATE,
  job_title VARCHAR(255),
  PRIMARY KEY (id)
);

 

insert into dataqualitypoc.dataquality.employee values (1, 'Sara', 'Mcguire', 'Female', 'tsharp@example.net', '(971)643-6089x9160', '08-17-2021', 'Editor, commissioning');

insert into dataqualitypoc.dataquality.employee values (2, 'Alisha', 'Hebert', 'Male', 'vincentgarrett@example.net', '+1-114-355-1841x78347', '08-17-2021', 'Broadcast engineer');

insert into dataqualitypoc.dataquality.employee values (3, 'Gwendolyn', 'Sheppard', 'Male', 'mercadojonathan@example.com', '9017807728', '09-25-2015', 'Industrial buyer');

insert into dataqualitypoc.dataquality.employee values (4, 'Collin', 'Allison', 'Male', 'yvaughn@example.net', '(314)591-7413', '11-21-1979', null);

insert into dataqualitypoc.dataquality.employee values (5, 'Gwendolyn', 'Sheppard', 'Male', 'mercadojonathan@example.com', '9017807728', '09-25-2015', 'Industrial buyer');

select * from  dataqualitypoc.dataquality.employee;

select max(run_id)+1 from dataqualitypoc.dataquality.data_compare_log;

select count(*) from dataqualitypoc.dataquality.employee where job_title is null;

--delete  from dataqualitypoc.dataquality.data_compare_log;

select * from dataqualitypoc.dataquality.data_compare_log;

select * from dataqualitypoc.dataquality.runstats;

select * from dataqualitypoc.dataquality.data_compare_check;

select * from dataquality.data_compare_diff;

select * from dataqualitypoc.dataquality.data_compare_log;