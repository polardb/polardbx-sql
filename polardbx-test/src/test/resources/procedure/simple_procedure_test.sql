### 测试输入输出参数 ###

  create database if not exists test;

  use test;

  drop procedure if exists pro_test;

  delimiter //;

  CREATE PROCEDURE test.pro_test()
  BEGIN
  BEGIN
  prepare ins from "select ?";
  execute ins using @x;
  execute ins using @y;
  select 3 + 3;
  deallocate prepare ins;
  END;
  END;//

  delimiter ;//

  call pro_test();

  drop procedure if exists pro_test;

  drop procedure if exists pro_test;

  delimiter //;

  CREATE PROCEDURE pro_test()
  BEGIN
  BEGIN
  show databases;
  END;
  END;//

  delimiter ;//

  call pro_test();

  # 测试输入输出参数
  delimiter //;

  drop procedure if exists pro_test//

  CREATE PROCEDURE pro_test(in val1 int, out val2 int, inout val3 int)
  BEGIN
  BEGIN
  WHILE val1 > 98 do
  SET val2 = val2 + 1;
  SET val3 = val3 + 2;
  SET val1 = val1 - 1;
  END WHILE;
  END;
  END;//

  delimiter ;//

  set @v1 = 100;
  set @v2 = 0;
  set @v3 = 0;

  call pro_test(@v1, @v2, @v3);

  ## 预期结果
  select @v1;
  ### -> 100 @v1不变, in参数无法传递到存储过程外部
  select @v2;
  ### -> null out参数无法传递至存储过程内部
  select @v3;
  ### -> 4 inout参数对外可见

  drop procedure pro_test;

  ### 测试If 语句 ###
  # 测试控制流程
  drop table if exists t1;
  drop view if exists t1;
  create table t1(id int, num int);

  ## 测试if语句
  delimiter //;

  CREATE PROCEDURE pro_test(in val1 int)
  BEGIN
  DECLARE x INTEGER DEFAULT 1;
  IF val1 = 1 THEN
  INSERT INTO t1 VALUES (x + 1,0);
  ELSE
  set x = x + 1;
  UPDATE t1 SET num = num + 1 WHERE id = x;
  END IF;
  END;//

  drop procedure if exists pro_test//

  CREATE PROCEDURE pro_test(in val1 varchar(20))
  BEGIN
  DECLARE x INTEGER DEFAULT 1;
  IF val1 = 'a' THEN
  INSERT INTO t1 VALUES (x + 1,0);
  ELSE
  set x = x + 1;
  UPDATE t1 SET num = num + 1 WHERE id = x;
  END IF;
  END;//

  drop procedure if exists pro_test//

  CREATE PROCEDURE pro_test(in val1 int)
  BEGIN
  DECLARE x INTEGER DEFAULT 1;
  IF !val1 THEN
  INSERT INTO t1 VALUES (x + 1,0);
  ELSE
  set x = x + 1;
  UPDATE t1 SET num = num + 1 WHERE id = x;
  END IF;
  END;//

  delimiter ;//

  call pro_test(1);
  select * from t1;

  ##	+----+-----+
  ##	| id | num |
  ##	+----+-----+
  ##	|  2 |   0 |
  ##	+----+-----+

  call pro_test(2);
  select * from t1;

  ##	+----+-----+
  ##	| id | num |
  ##	+----+-----+
  ##	|  2 |   1 |
  ##	+----+-----+

  call pro_test(3);
  select * from t1;

  ##	+----+-----+
  ##	| id | num |
  ##	+----+-----+
  ##	|  2 |   2 |
  ##	+----+-----+

  drop procedure pro_test;
  delete from t1;

  ### 测试case语句 ###
  ## 测试case语句（匹配条件）
  delimiter //;

  CREATE PROCEDURE pro_test(in val1 int)
  BEGIN
  DECLARE x INTEGER DEFAULT 1;
  CASE val1
  WHEN 1 THEN
  INSERT INTO t1 VALUES (val1, x + 1);
  WHEN 2 THEN
  DELETE FROM t1 WHERE id = 1;
  ELSE
  UPDATE t1 SET num = num + 1 WHERE id = x;
  END CASE;
  END;//

  delimiter ;//

  call pro_test(1);
  select * from t1;

  ##	+----+-----+
  ##	| id | num |
  ##	+----+-----+
  ##	|  1 |   2 |
  ##	+----+-----+

  call pro_test(2);
  select * from t1;

  ## empty

  call pro_test(1);
  call pro_test(3);
  select * from t1;

  ## +----+-----+
  ## | id | num |
  ## +----+-----+
  ## |  1 |   3 |
  ## +----+-----+

  drop procedure pro_test;
  delete from t1;

  ## 测试case语句（搜索条件）
  delimiter //;

  CREATE PROCEDURE pro_test(in val1 int)
  BEGIN
  DECLARE x INTEGER DEFAULT 1;
  CASE
  WHEN x > val1 THEN INSERT INTO t1 VALUES (x + val1, 1);
  WHEN x = val1 THEN INSERT INTO t1 VALUES (x - val1, 1);
  ELSE INSERT INTO t1 VALUES (val1, 1);
  END CASE;
  END;//

  delimiter ;//

  call pro_test(1);
  select * from t1;

  ## +----+-----+
  ## | id | num |
  ## +----+-----+
  ## |  0 |   1 |
  ## +----+-----+

  call pro_test(2);
  select * from t1;

  ## +----+-----+
  ## | id | num |
  ## +----+-----+
  ## |  0 |   1 |
  ## |  2 |   1 |
  ## +----+-----+

  call pro_test(0);
  select * from t1;

  ## +----+-----+
  ## | id | num |
  ## +----+-----+
  ## |  0 |   1 |
  ## |  2 |   1 |
  ## |  1 |   1 |
  ## +----+-----+

  drop procedure pro_test;
  delete from t1;

  ### 测试loop语句 ###
  ## 测试loop语句
  delimiter //;

  drop procedure if exists pro_test//

  CREATE PROCEDURE pro_test(inout x INT)
  LOOP
    SET x = x + 1;
    END LOOP//

  drop procedure if exists pro_test//

  ?expect_exception? error: should not empty
  CREATE PROCEDURE pro_test(inout x INT)//

  drop procedure if exists pro_test//

  CREATE PROCEDURE pro_test(inout x INT)
  BEGIN
  label1: LOOP
    SET x = x + 1;
    IF x < 10 THEN
    ITERATE label1;
    END IF;
    LEAVE label1;
    END LOOP label1;
    END;//

  delimiter ;//

  set @v1 = 1;
  call pro_test(@v1);
  select @v1;
  # 返回结果为10

  set @v1 = 3;
  call pro_test(@v1);
  select @v1;
  # 返回结果为10

  drop procedure pro_test;

  ### 测试repeat语句 ###
  ## 测试repeat语句
  delimiter //;

  CREATE PROCEDURE pro_test(inout x INT)
  BEGIN
  SET x = 0;
  REPEAT
  SET x = x + 1;
  UNTIL x > 10
  END REPEAT;
  END;//

  delimiter ;//

  call pro_test(@v2);
  select @v2;
  # 返回结果为11

  drop procedure pro_test;

  ### 测试调用其他的存储过程 ###

  drop procedure if exists pro_test1;
  ## 调用其他的存储过程
  drop table if exists t1;
  create table t1(id int, num int);

  delimiter //;

  CREATE PROCEDURE pro_test1(in val1 int)
  BEGIN
  DECLARE x INTEGER DEFAULT 1;
  IF val1 = 1 THEN
  INSERT INTO t1 VALUES (x + 1,0);
  ELSE
  set x = x + 1;
  UPDATE t1 SET num = num + 1 WHERE id = x;
  END IF;
  END;//

  CREATE PROCEDURE pro_test(in val1 int)
  BEGIN
  DECLARE x INTEGER DEFAULT 1;
  CASE val1
  WHEN 1 THEN
  call pro_test1(1);
  WHEN 2 THEN
  DELETE FROM t1 WHERE id = 1;
  ELSE
  UPDATE t1 SET num = num + 1 WHERE id = x;
  END CASE;
  END;//

  delimiter ;//

  call pro_test(1);
  ## 返回的affect rows为1

  ## 查看t1表是否有数据插入
  select * from t1;
  ## +----+------+
  ## | id | data |
  ## +----+------+
  ## |  2 | 0    |
  ## +----+------+

  drop procedure pro_test;
  drop procedure if exists pro_test1;

  drop table if exists t1;

  create table t1(id int, num int);

  insert into t1 values (1,1);

  delimiter //;

  CREATE PROCEDURE pro_test(in val1 int)
  BEGIN
  DECLARE x INTEGER DEFAULT 1;
  UPDATE t1 SET num = num + x WHERE id = 1;
  BEGIN
  DECLARE x INTEGER DEFAULT 100;
  UPDATE t1 SET num = num + x WHERE id = 1;
  END;
  END;//

  delimiter ;//

  SELECT * FROM t1;

  call pro_test(1);

  SELECT * FROM t1;

  drop table if exists t;

  create table t (a varchar(255), b int);

  drop procedure if exists pro_test;

  delimiter |;

  create procedure pro_test()
  begin
  declare x varchar(255) default '"test\'""''test';
  select concat(concat(`a`, '123'), x) from t;
  insert into t values (x, 1);
  end;|

  call pro_test()|

  select * from t|

  call pro_test()|

  select * from t|

  drop procedure if exists pro_test|

  drop table if exists t1|
  drop table if exists t2|
  drop table if exists t3|

  create table t1(id int, data char(16))|
  create table t2(id int, data char(16))|
  create table t3(id int, data char(16))|

  insert into t1 values (3, 't1'), (2, 't1')|
  insert into t2 values (1, 't2')|

  ?expect_exception? leave:
  CREATE PROCEDURE pro_test()
  BEGIN
  DECLARE a CHAR(16);
  DECLARE b, c int;
  DECLARE cur1 CURSOR FOR SELECT data, id FROM t1 order by id;
  DECLARE cur2 CURSOR FOR SELECT id FROM t2 order by id;
  DECLARE CONTINUE HANDLER FOR NOT FOUND begin LEAVE read_loop; end;

  OPEN cur1;
  OPEN cur2;

  read_loop: LOOP
    FETCH cur1 INTO a, b;
    FETCH cur2 INTO c;
    IF b < c THEN
    INSERT INTO t3 VALUES (b, a);
    ELSE
    INSERT INTO t3 VALUES (c, a);
    END IF;
  END LOOP;

  CLOSE cur1;
  CLOSE cur2;
  END;|

  ?expect_exception? not:
  call pro_test()|

  drop procedure if exists pro_test|

  CREATE PROCEDURE pro_test(inout x INT)
  BEGIN
  label1: LOOP
    SET x = x + 1;
    IF x < 10 THEN
    ITERATE label1;
    END IF;
    LEAVE Label1;
  END LOOP label1;
  set @vx = 12;
  END|

  set @v1 = 1|

  call pro_test(@v1)|

  select @vx|

  drop procedure if exists pro_test|

  CREATE PROCEDURE pro_test()
  BEGIN
  declare x int default 0;
  set @vx = X;
  END;|

  call pro_test()|

  select @vx|

  drop procedure if exists pro_test|

  ?expect_exception? Duplicate: Duplicate
  CREATE PROCEDURE pro_test(y int, out y int)
  BEGIN
  declare x int default 0;
  set @vx = X;
  END;|

  drop procedure if exists pro_test|

  ?expect_exception? Duplicate: duplicate
  CREATE PROCEDURE pro_test()
  BEGIN
  DECLARE cur1 CURSOR FOR SELECT 1;
  DECLARE cur1 CURSOR FOR SELECT 2;
  set @vx = 3;
  END;|

  drop procedure if exists pro_test|

  ?expect_exception? Duplicate: duplicate
  CREATE PROCEDURE pro_test()
  BEGIN
  declare x int default 0;
  declare x int default 1;
  set @vx = X;
  END;|

  ?expect_exception? error: duplicate
  CREATE PROCEDURE pro_test()
  declare x int default 0;
  declare X int default 1;
  set @vx = X;|

  drop procedure if exists pro_test|

  drop table if exists t|

  create table t(data char(25))|

  CREATE PROCEDURE pro_test()
  BEGIN
  declare x char(25) default '"xxx"';
  insert into t values (x);
  END;|

  call pro_test|

  select * from t|

  drop procedure if exists pro_test|

  CREATE PROCEDURE pro_test()
  BEGIN
  declare x char(25) default 'xxx';
  IF x = 'XXX' then set @vx = 123;
  end if;
  END;|

  select @vx|

  drop procedure if exists pro_test|

  create procedure pro_test()
  set @@autocommit = '10'|

  ?expect_exception? autocommit: autocommit
  call pro_test|

  select 1|

  ?expect_exception? error: not support
  explain call pro_test|

  drop procedure if exists pro_test|

  ?expect_exception? error: should not empty
CREATE PROCEDURE pro_test(in val1 int)
  BEGIN
  BEGIN
  WHILE val1 > 98 do
  END WHILE;
  END;
  END;|

  ?expect_exception? error: should not empty
CREATE PROCEDURE pro_test(in val1 int)
  BEGIN
  BEGIN
  LOOP
  END LOOP;
  END;
  END;|

  ?expect_exception? error: should not empty
CREATE PROCEDURE pro_test(in val1 int)
  BEGIN
  BEGIN
  REPEAT
  UNTIL val1 > 100
  END REPEAT;
  END;
  END;|
delimiter ;|

drop procedure if exists pro_test;

create procedure pro_test()
  set session autocommit = 0;

call pro_test();

drop procedure if exists pro_test;

  create procedure pro_test()
  set @@autocommit = 0;

call pro_test();

drop procedure if exists pro_test;

  create procedure pro_test()
  set @@session.autocommit = 0;

call pro_test();

delimiter |;

drop procedure if exists pro_test|

create procedure pro_test()
begin
update test_procedure set c2 = 101 where c1 = 1;
if row_count() > 0 then
    set @x = 2;
end if;
update test_procedure set c2 = 101 where c1 = 101;
if row_count() > 0 then
    set @z = 2;
end if;
select @x,@z;
end|

drop table if exists test_procedure|
create table test_procedure (c1 int, c2 varchar(20))|
insert into test_procedure values (1,"1"), (2,"2")|

call pro_test|

select @x, @z|

drop procedure if exists pro_test|

drop table if exists test_procedure|
create table test_procedure (c1 int, c2 varchar(20))|
insert into test_procedure values (1,"1"), (2637,"2")|

create procedure pro_test()
begin
select * from test_procedure where c2 = 1;
if found_rows() > 0 then
    set @x = 3;
end if;
select * from test_procedure where c2 = 102;
if found_rows() > 0 then
    set @z = 3;
end if;
select @x,@z;
end|

call pro_test|
select @x, @z|

drop table if exists test_procedure|
drop procedure if exists pro_test|
create procedure pro_test()
begin
declare aa char(20);
declare bb char(20);
declare cc char(20);
declare dd char(20);
declare xx varchar(255);
declare yy varchar(255);
declare zz varchar(255);
declare idx int default 1;
set aa = "xx,yy,z1-z2";
set bb = "cc,zz";
set cc = "aa";
set xx = substring_index(aa, ",", idx);
set yy = substring_index(substring_index(aa, ",", -idx), "-", idx);
set zz = concat_ws(",", aa, bb, cc, dd);
select xx, yy, zz into @x, @y, @z;
end;|

call pro_test|
select @x, @y, @z|
drop procedure if exists pro_test|

# test blob as input param
drop table if exists t_param_test|
create table t_param_test(c1 char(255), c2 longblob, c3 blob, c4 tinyblob, c5 mediumblob);|
create procedure pro_test(in data longblob) begin insert into t_param_test values ('111', data, data, data, data); end;|
call pro_test(0xa4a5)|
select * from t_param_test|
drop procedure if exists pro_test|
drop table if exists t_param_test|
