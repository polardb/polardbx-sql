create database if not exists test;

use test;

delimiter |;

drop table if exists procedure_exception_handler_test|

create table procedure_exception_handler_test(data int)|

drop procedure if exists bug2260|

create procedure bug2260()
begin
  declare v1 int;
  declare c1 cursor for select data from procedure_exception_handler_test;
declare continue handler for not found set @x2 = 1;

open c1;
fetch c1 into v1;
set @x2 = 2;
close c1;
end|

call bug2260()|
select @x2|
drop procedure bug2260|

create procedure bug2260()
begin
  declare v1 int;
  declare continue handler for 4006 set @x2 = 1;
  select data from dsajlgal_asadngka;

end|

?expect_exception? doesn't exist :
call bug2260()|
drop procedure bug2260|

create procedure bug2260()
begin
  declare v1 int;
  declare validate_error condition for 4006;
  declare continue handler for validate_error set @x2 = 1;
select data from dsajlgal_asadngka;

end|

?expect_exception? doesn't exist :
call bug2260()|
drop procedure bug2260|

drop procedure if exists bug2260|
create procedure bug2260()
begin
  declare v1 int;
  declare c1 cursor for select data from procedure_exception_handler_test;
declare exit handler for not found set @x2 = 1;

open c1;
fetch c1 into v1;
set @x2 = 2;
close c1;
end|

call bug2260()|
select @x2|
drop procedure bug2260|

drop procedure if exists bug9856|
create procedure bug9856()
begin
  declare v int;
  declare c cursor for select data from procedure_exception_handler_test;
declare exit handler for not found select '16';

open c;
fetch c into v;
select v;
end|

delete from procedure_exception_handler_test|
call bug9856()|
call bug9856()|
drop procedure bug9856|

drop procedure if exists bug11529|
drop table if exists procedure_exception_handler_test2|

create table procedure_exception_handler_test2 (
                    id   char(16) not null default '',
                    data int not null
)|

create procedure bug11529()
begin
  declare c cursor for select id, data from procedure_exception_handler_test2 where data in (10,13);

open c;
begin
    declare vid char(16);
    declare vdata int;
    declare exit handler for not found begin end;

    while true do
      fetch c into vid, vdata;
end while;
end;
close c;
end|

insert into procedure_exception_handler_test2 values
  ('Name1', 10),
  ('Name2', 11),
  ('Name3', 12),
  ('Name4', 13),
  ('Name5', 14)|

call bug11529()|
call bug11529()|
delete from procedure_exception_handler_test2|
drop procedure bug11529|

drop procedure if exists bug12168|
drop table if exists t3|
drop table if exists t4|


create table t3 (a int)|
insert into t3 values (1),(2),(3),(4)|

create table t4 (a int)|

create procedure bug12168(arg1 char(1))
begin
  declare b, c integer;
  if arg1 = 'a' then
begin
      declare c1 cursor for select a from t3 where a % 2;
declare continue handler for not found set b = 1;
      set b = 0;
open c1;
c1_repeat: repeat
        fetch c1 into c;
        if (b = 1) then
          leave c1_repeat;
end if;

insert into t4 values (c);
until b = 1
end repeat;
end;
end if;
  if arg1 = 'b' then
begin
      declare c2 cursor for select a from t3 where not a % 2;
declare continue handler for not found set b = 1;
      set b = 0;
open c2;
c2_repeat: repeat
        fetch c2 into c;
        if (b = 1) then
          leave c2_repeat;
end if;

insert into t4 values (c);
until b = 1
end repeat;
end;
end if;
end|

call bug12168('a')|
select * from t4|
truncate t4|
call bug12168('b')|
select * from t4|
truncate t4|
call bug12168('a')|
select * from t4|
truncate t4|
call bug12168('b')|
select * from t4|
truncate t4|
drop table t3|
drop table t4|
drop procedure if exists bug12168|

drop table if exists procedure_exception_handler_test|

drop table if exists t_33618|
drop procedure if exists proc_33618|

create table t_33618 (`a` int, unique(`a`), `b` varchar(30)) engine=myisam|
insert into t_33618 (`a`,`b`) values (1,'1'),(2,'2')|

create procedure proc_33618(num int)
begin
  declare count1 int default '0';
  declare vb varchar(30);
  declare last_row int;

  while(num>=1) do
    set num=num-1;
begin
      declare cur1 cursor for select `a` from t_33618;
declare continue handler for not found set last_row = 1;
      set last_row:=0;
open cur1;
rep1:
      repeat
begin
          declare exit handler for 1062 begin end;
fetch cur1 into vb;
if (last_row = 1) then
            leave rep1;
end if;
end;
        until last_row=1
end repeat;
close cur1;
end;
end while;
end|

delimiter ;|

call proc_33618(20);

drop table t_33618;
drop procedure proc_33618;

drop table if exists t1;
drop table if exists t2;
drop view if exists t1;
DROP PROCEDURE if exists pc;
DROP PROCEDURE if exists pc_with_flush;
CREATE TABLE t1(a INTEGER);
CREATE TABLE t2(a INTEGER, b INTEGER);
INSERT INTO t1 VALUES(0), (1), (2);
INSERT INTO t2 VALUES(1, 10), (2, 20), (2, 21);
DELIMITER //;
CREATE PROCEDURE pc(val INTEGER)
BEGIN
  DECLARE finished, col_a, col_b INTEGER DEFAULT 0;
  DECLARE c CURSOR FOR
SELECT a, (SELECT b FROM t2 WHERE t1.a=t2.a) FROM t1 WHERE a = val;
DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished = 1;
  SET finished = 0;
OPEN c;
loop1: LOOP
    FETCH c INTO col_a, col_b;
    IF finished = 1 THEN
      LEAVE loop1;
END IF;
END LOOP loop1;
CLOSE c;
END //
CREATE PROCEDURE pc_with_flush()
BEGIN
  DECLARE finished, col_a, col_b INTEGER DEFAULT 0;
  DECLARE val INTEGER DEFAULT 0;
  DECLARE c CURSOR FOR
SELECT a, (SELECT b FROM t2 WHERE t1.a=t2.a) FROM t1 WHERE a = val;
DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished = 1;
  SET finished = 0;
OPEN c;
loop1: LOOP
    FETCH c INTO col_a, col_b;
    IF finished = 1 THEN
      LEAVE loop1;
END IF;
END LOOP loop1;
CLOSE c;
FLUSH TABLES;
  SET finished = 0;
  SET val = 1;
OPEN c;
loop2: LOOP
    FETCH c INTO col_a, col_b;
    IF finished = 1 THEN
      LEAVE loop2;
END IF;
END LOOP loop2;
CLOSE c;
FLUSH TABLES;
  SET val = 2;
  SET finished = 0;
OPEN c;
loop3: LOOP
    FETCH c INTO col_a, col_b;
    IF finished = 1 THEN
      LEAVE loop3;
END IF;
END LOOP loop3;
CLOSE c;
END //
DELIMITER ;//
CALL pc(1);
?expect_exception? more than 1 row: more than 1 row
CALL pc(2);
DROP PROCEDURE pc;
DROP PROCEDURE pc_with_flush;
DROP TABLE t1;
drop table t2;