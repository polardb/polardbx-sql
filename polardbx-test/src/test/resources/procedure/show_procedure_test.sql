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

drop procedure if exists `test`.`p1`;