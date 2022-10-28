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