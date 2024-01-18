create database if not exists test;

use test;

drop function if exists f1;

set global log_bin_trust_function_creators = on;

create function f1() returns int return 1;
drop function f1;

create function `f1`() returns int return 1;
drop function `f1`;

create function test.`f1`() returns int return 1;
drop function if exists test.`f1`;

create function `test`.`f1`() returns int return 1;

select f1();
select `f1`();
select test.`f1`();
select `test`.`f1`();

show create function f1;
show create function `f1`;
show create function `test`.f1;
show create function `test`.`f1`;

show function status like '%f1%';
show function status where name like '%f1%';

alter function test.f1 COMMENT 'testxxx';
select IS_DETERMINISTIC,SECURITY_TYPE,SQL_DATA_ACCESS,ROUTINE_COMMENT from information_schema.routines where SPECIFIC_NAME = 'f1' or SPECIFIC_NAME = 'mysql.f1';

alter function test.f1 SQL SECURITY INVOKER;
select IS_DETERMINISTIC,SECURITY_TYPE,SQL_DATA_ACCESS,ROUTINE_COMMENT from information_schema.routines where SPECIFIC_NAME = 'f1' or SPECIFIC_NAME = 'mysql.f1';

alter function test.f1 COMMENT 'testxxx' LANGUAGE SQL SQL SECURITY INVOKER;

show create function f1;
show function status where name like '%f1%';
select IS_DETERMINISTIC,SECURITY_TYPE,SQL_DATA_ACCESS,ROUTINE_COMMENT from information_schema.routines where SPECIFIC_NAME = 'f1' or SPECIFIC_NAME = 'mysql.f1';

drop function if exists `test`.`f1`;