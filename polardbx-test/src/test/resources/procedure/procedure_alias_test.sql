drop procedure if exists pro_test;

  delimiter |;

  create procedure pro_test()
  begin
  declare x int default 1;
  select 1 + x;
  select x + 1;
  select x + 1 + x;
  select 'te?st?' + x;
  select '?' + x + "?";
  select '\'' + x + '\"?';
  select 'te?st?' + x;
  end|

  delimiter ;|

  call pro_test();

  drop procedure if exists pro_test;

  delimiter |;

  create procedure pro_test()
  begin
  declare x int default 1;
  select 1 + x as '1 + x';
  select x + 1 as "x + 1";
  select x + 1 + x as 'x + 1 + x';
  select 'te?st?' + x as "'te?st?' + x";
  select '?' + x + "?" as `'?' + x + "?"`;
  select '\'' + x + '\"?' as `'\'' + x + '\"?'`;
  select 'te?st?' + x AS '''te?st?'' + x';
  end|

  delimiter ;|

  call pro_test();

  drop table if exists t;

  create table t (a int , b int);

  insert into t values (1,1), (2,2);

  drop procedure if exists pro_test;

  delimiter |;

  create procedure pro_test()
  begin
  declare x int default 1;
  select concat(concat(a, '123'), x) from t;
  end|

  call pro_test()|

  drop procedure if exists pro_test|

  create procedure pro_test()
  begin
  declare x int default 1;
  select concat(concat(`a`, '123'), x) from t;
  end|

  call pro_test()|

drop procedure if exists pro_test|

drop procedure if exists `test-1`|

drop table if exists `test-2`|

create table `test-2` (`a-1` int)|

insert into `test-2` values (1)|

create procedure `test-1` ()
comment 'test-xx1'
begin
select `a-1`, count(*) from `test-2` order by `a-1`;
end|

call `test-1`()|

drop procedure if exists `test-3`|

create procedure `test-3` ()
    comment 'test-xx3'
begin
select 1;
call `test-1`;
end|

drop procedure `test-1`|

drop procedure if exists `test-3`|

drop table if exists `test-2`|