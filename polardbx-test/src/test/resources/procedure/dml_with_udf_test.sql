create database if not exists test;

use test;
drop table if exists udf_dml_test;
drop function if exists my_add;

create function my_add(num int) returns int return num + 1;
create table udf_dml_test(c1 int primary key, c2 int, c3 varchar(255));

insert into udf_dml_test values (1, my_add(20), convert(my_add(100) + 200, char(100)));
select * from udf_dml_test;

# insert into udf_dml_test values (1, my_add(20), convert(my_add(100) + 200, char(100))) on duplicate key update c2 = my_add(c2) + 100, c3 = cast(my_add(c2) as char(255));

insert ignore into udf_dml_test values (2, my_add(20), convert(my_add(123) + 200, char(100)));
select * from udf_dml_test;

replace into udf_dml_test values (2, my_add(21), convert(my_add(113) + 200, char(100)));
select * from udf_dml_test;

update udf_dml_test set c2 = 2 where c1 = my_add(0);
select * from udf_dml_test;

delete from udf_dml_test where c1 = my_add(0);
select * from udf_dml_test;