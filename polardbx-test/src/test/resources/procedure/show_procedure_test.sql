create database if not exists test;

use test;

drop procedure if exists p1;

create procedure p1() begin end;
drop procedure p1;

create procedure `p1`() begin end;
drop procedure `p1`;

create procedure test.`p1`() begin end;
drop procedure if exists test.`p1`;

create procedure `test`.`p1`() begin end;

call p1();
call `p1`();
call test.`p1`();
call `test`.`p1`();

show create procedure p1;
show create procedure `p1`;
show create procedure `test`.p1;
show create procedure `test`.`p1`;

show procedure status like '%p1%';
show procedure status where name like '%p1%';

alter procedure test.p1 COMMENT 'testxxx1';
select IS_DETERMINISTIC,SECURITY_TYPE,SQL_DATA_ACCESS,ROUTINE_COMMENT from information_schema.routines where SPECIFIC_NAME = 'p1' and ROUTINE_SCHEMA = 'test';

alter procedure test.p1 LANGUAGE SQL;
select IS_DETERMINISTIC,SECURITY_TYPE,SQL_DATA_ACCESS,ROUTINE_COMMENT from information_schema.routines where SPECIFIC_NAME = 'p1' and ROUTINE_SCHEMA = 'test';

alter procedure test.p1 MODIFIES SQL DATA;
select IS_DETERMINISTIC,SECURITY_TYPE,SQL_DATA_ACCESS,ROUTINE_COMMENT from information_schema.routines where SPECIFIC_NAME = 'p1' and ROUTINE_SCHEMA = 'test';

alter procedure test.p1 SQL SECURITY INVOKER;
select IS_DETERMINISTIC,SECURITY_TYPE,SQL_DATA_ACCESS,ROUTINE_COMMENT from information_schema.routines where SPECIFIC_NAME = 'p1' and ROUTINE_SCHEMA = 'test';

alter procedure test.p1 COMMENT 'testxxx' LANGUAGE SQL MODIFIES SQL DATA SQL SECURITY INVOKER;

show create procedure p1;
show procedure status where name like '%p1%';
select IS_DETERMINISTIC,SECURITY_TYPE,SQL_DATA_ACCESS,ROUTINE_COMMENT from information_schema.routines where SPECIFIC_NAME = 'p1' and ROUTINE_SCHEMA = 'test';

drop procedure if exists `test`.`p1`;