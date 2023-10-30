create database if not exists test;

use test;

delimiter |;

drop procedure if exists _testProcXXXxzZyY_|

Create procedure _testProcXXXxzZyY_ (
    in in_param1 int,
    in in_paramXxZy2 LONGBLOB,
    out checkThisValue int
)
    return_label:
begin
    DECLARE c1sagdaC int;
    declare dajgkajs LONGBLOB;
    DECLARE test_count INT;

START TRANSACTION;
set checkThisValue=1;
delete from _table_test_cxdasdga where localc1=in_param1;
select ROW_COUNT() into test_count;
if test_count = 0 then
      set checkThisValue=2;
ROLLBACK;
LEAVE return_label;
end if;
    set c1sagdaC=0;
select count(*) INTO c1sagdaC from _table_test_cxdasdga;
if c1sagdaC = 0 then
      set dajgkajs = BINARY '';
delete from _table_xxcadgs_Test_xca where c1=dajgkajs;
else
    set checkThisValue=0;
    LEAVE return_label;
    if 1=1 then
    COMMIT;
end if;
end if;
end;|

drop procedure if exists _atest_dalgalnkjds|

Create procedure _atest_dalgalnkjds (
    IN in_pram1 VARBINARY(767),
    IN in_pram2 MEDIUMBLOB,
    INOUT out_param bigint,
    OUT checkThisValue int
        )
    return_label:begin
DECLARE oldversionexsit BIGINT;
DECLARE test_count int;
START TRANSACTION;
set checkThisValue=1;
select version INTO oldversionexsit from _table_xxcadgs_Test_xca where c1=in_pram1;
select version from _table_xxcadgs_Test_xca where c1=in_pram1;
select FOUND_ROWS();
select FOUND_ROWS() into test_count;
select test_count;
select count(version) into test_count from _table_xxcadgs_Test_xca where c1=in_pram1;
select test_count;
if test_count > 0 then
	if oldversionexsit <> out_param then
		set checkThisValue=2;
select 'x1';
ROLLBACK;
LEAVE return_label;
end if;
	set oldversionexsit = oldversionexsit + 1;
select 'x2';
update _table_xxcadgs_Test_xca set nsd=in_pram2, version=oldversionexsit where c1=in_pram1;
select ROW_COUNT() into test_count;
if test_count = 1 then
select 'x3';
set out_param = oldversionexsit;
		set checkThisValue=0;
COMMIT;
LEAVE return_label;
end if;
	set checkThisValue=3;
select 'x4';
ROLLBACK;
LEAVE return_label;
end if;
select 'x5';
insert into _table_xxcadgs_Test_xca values(in_pram1,in_pram2,out_param);
select 'x5';
select ROW_COUNT() into test_count;
if test_count = 1 then
select 'x6';
set checkThisValue=0;
COMMIT;
LEAVE return_label;
end if;
set checkThisValue=4;
select 'x7';
ROLLBACK;
LEAVE return_label;
end;|

drop procedure if exists _atest_dalgalnkjds|

Create procedure _atest_dalgalnkjds (
    IN    in_pram1 VARBINARY(767),
    IN    in_pram2 MEDIUMBLOB,
    INOUT out_param bigint,
    OUT   checkThisValue int
        )
    return_label:begin
                        DECLARE oldversionexsit BIGINT;
                        DECLARE test_count int;

START TRANSACTION;
set checkThisValue=1;
select version INTO oldversionexsit from _table_xxcadgs_Test_xca where c1=in_pram1;
select FOUND_ROWS() into test_count;
if test_count > 0 then
                            if oldversionexsit <> out_param then
                                set checkThisValue=2;
ROLLBACK;
LEAVE return_label;
end if;
                            set oldversionexsit = oldversionexsit + 1;
update _table_xxcadgs_Test_xca set nsd=in_pram2, version=oldversionexsit where c1=in_pram1;
select ROW_COUNT() into test_count;
if test_count = 1 then
                                set out_param = oldversionexsit;
                                set checkThisValue=0;
COMMIT;
LEAVE return_label;
end if;
end if;

insert into _table_xxcadgs_Test_xca values(in_pram1,in_pram2,out_param);
select ROW_COUNT() into test_count;
if test_count = 1 then
                            set checkThisValue=0;
COMMIT;
LEAVE return_label;
end if;
                        set checkThisValue=4;
ROLLBACK;
LEAVE return_label;
end;|

drop procedure if exists `dajladg_dsagdkn`|

CREATE PROCEDURE `dajladg_dsagdkn`(IN i_dsaklgac1 VARCHAR(50))
    top:begin


declare param1 varchar(50);
declare param2 datetime;
declare param3 datetime;
declare param4 int default 0;
declare param5 varchar(50) default 'dajladg_dsagdkn';


select UUID() into param1 from dual;
select NOW() into param2 from dual;


if i_dsaklgac1 is null or i_dsaklgac1 = '' then


select NOW() into param3 from dual;
insert into dagalgmdalmsg_dagkn (c1, ProcName, aoppop, BeginTime, dshf, yuuy, ErrMsg, dsaklgac1, klsdgm, p_dsakg, dsagjk, daldag)
values (param1, param5, 3, param2, param3, TIMESTAMPDIFF(SECOND,param2,param3), '好像发生了什么', '不知道', 'sql', param2, 'sql', param3);

leave top;

end if;


select count(1) into param4 from dagalgmdalmsg_dagkn where aoppop = 1 and dsaklgac1 = '测试' and p_dsakg > date_sub(NOW(), interval 30 MINUTE);


if param4 != 0 then


select NOW() into param3 from dual;
insert into dagalgmdalmsg_dagkn (c1, ProcName, aoppop, BeginTime, dshf, yuuy, ErrMsg, dsaklgac1, klsdgm, p_dsakg, dsagjk, daldag)
values (param1, param5, 3, param2, param3, TIMESTAMPDIFF(SECOND,param2,param3), '这里只是测试一下', i_dsaklgac1, 'sql', param2, 'sql', param3);

leave top;

end if;


if param4 = 0 then


insert into dagalgmdalmsg_dagkn (c1, ProcName, aoppop, BeginTime, dshf, yuuy, ErrMsg, dsaklgac1, klsdgm, p_dsakg, dsagjk, daldag)
values (param1, param5, 1, param2, NULL, NULL, NULL, i_dsaklgac1, 'sql', param2, 'sql', param2);

start transaction;


delete from table_sadga_dsab_dagka where dsaklgac1 = i_dsaklgac1;


insert into table_sadga_dsab_dagka (c1, dsaklgac1, adgalag, cxcxXz, daal, klsdgm, p_dsakg, dsagjk, daldag)
select
    UUID() as c1,
    dsagldsaklga.c1 as dsaklgac1,
    tywqy.c1 as adgalag,
    tywqy.cxcxXz as cxcxXz,
    qwqe.Attr1 as daal,
    'sql' as klsdgm,
    NOW() as p_dsakg,
    'sql' as dsagjk,
    NOW() as daldag
from dsagldsaklga dsagldsaklga
         left join dsagllsdnsd qwqe on qwqe.c1 = dsagldsaklga.Attr5
         left join dsagldsaklgaStructure dsaklgastru on dsaklgastru.dsagldsaklgac1 = dsagldsaklga.c1
         left join dsaglOrgStructure orgstru on orgstru.dsaglStructurec1 = dsaklgastru.dsaglStructurec1
         left join dsaglOrganization tywqy on tywqy.c1 = orgstru.adgalag
where 1=1
  and tywqy.cxcxXz = 4
  and qwqe.Attr1 = '0'
  and dsagldsaklga.c1 = i_dsaklgac1
  and dsagldsaklga.dsaglcxcxXz = 99
  and dsagldsaklga.dsaklgaaoppop = 0;


insert into table_sadga_dsab_dagka (c1, dsaklgac1, adgalag, cxcxXz, daal, klsdgm, p_dsakg, dsagjk, daldag)
select
    UUID() as c1,
    dsagldsaklga.c1 as dsaklgac1,
    store.c1 as adgalag,
    store.cxcxXz as cxcxXz,
    qwqe.Attr1 as daal,
    'sql' as klsdgm,
    NOW() as p_dsakg,
    'sql' as dsagjk,
    NOW() as daldag
from dsagldsaklga dsagldsaklga
left join dsagllsdnsd qwqe on qwqe.c1 = dsagldsaklga.Attr5
    left join dsagldsaklgaStructure dsaklgastru on dsaklgastru.dsagldsaklgac1 = dsagldsaklga.c1
    left join dsaglOrgStructure orgstru on orgstru.dsaglStructurec1 = dsaklgastru.dsaglStructurec1
    left join dsaglOrganization tywqy on tywqy.c1 = orgstru.adgalag
    left join dsagllpoopcsli lpoop on lpoop.Parentc1 = tywqy.c1
    left join dsaglOrganization store on store.c1 = lpoop.Childc1
where 1=1
  and lpoop.ad = 1
  and tywqy.cxcxXz = 4
  and qwqe.Attr1 = '0'
  and dsagldsaklga.c1 = i_dsaklgac1
  and dsagldsaklga.dsaglcxcxXz = 99
  and dsagldsaklga.dsaklgaaoppop = 0
group by dsagldsaklga.c1, store.c1;


insert into table_sadga_dsab_dagka (c1, dsaklgac1, adgalag, cxcxXz, daal, klsdgm, p_dsakg, dsagjk, daldag)
select
    UUID() as c1,
    dsagldsaklga.c1 as dsaklgac1,
    tywqy.c1 as adgalag,
    tywqy.cxcxXz as cxcxXz,
    qwqe.Attr1 as daal,
    'sql' as klsdgm,
    NOW() as p_dsakg,
    'sql' as dsagjk,
    NOW() as daldag
from dsagldsaklga dsagldsaklga
         left join dsagllsdnsd qwqe on qwqe.c1 = dsagldsaklga.Attr5
         left join dsagldsaklgacsli dsaklgaorg on dsaklgaorg.dsaklgac1 = dsagldsaklga.c1
         left join dsaglOrganization tywqy on tywqy.c1 = dsaklgaorg.adgalag
where 1=1
  and tywqy.cxcxXz = 4
  and qwqe.Attr1 = '1'
  and dsagldsaklga.c1 = i_dsaklgac1
  and dsagldsaklga.dsaglcxcxXz = 99
  and dsagldsaklga.dsaklgaaoppop = 0;


insert into table_sadga_dsab_dagka (c1, dsaklgac1, adgalag, cxcxXz, daal, klsdgm, p_dsakg, dsagjk, daldag)
select
    UUID() as c1,
    dsagldsaklga.c1 as dsaklgac1,
    store.c1 as adgalag,
    store.cxcxXz as cxcxXz,
    qwqe.Attr1 as daal,
    'sql' as klsdgm,
    NOW() as p_dsakg,
    'sql' as dsagjk,
    NOW() as daldag
from dsagldsaklga dsagldsaklga
         left join dsagllsdnsd qwqe on qwqe.c1 = dsagldsaklga.Attr5
         left join dsagldsaklgacsli dsaklgaorg on dsaklgaorg.dsaklgac1 = dsagldsaklga.c1
         left join dsaglOrganization tywqy on tywqy.c1 = dsaklgaorg.adgalag
         left join dsagllpoopcsli lpoop on lpoop.Parentc1 = tywqy.c1
         left join dsaglOrganization store on store.c1 = lpoop.Childc1
where 1=1
  and lpoop.ad = 1
  and tywqy.cxcxXz = 4
  and qwqe.Attr1 = '1'
  and dsagldsaklga.c1 = i_dsaklgac1
  and dsagldsaklga.dsaglcxcxXz = 99
  and dsagldsaklga.dsaklgaaoppop = 0
group by dsagldsaklga.c1, store.c1;

commit;


select NOW() into param3 from dual;
update dagalgmdalmsg_dagkn set aoppop = 2, dshf = param3, yuuy = TIMESTAMPDIFF(SECOND,param2,param3), dsagjk = 'sql', daldag = param3 where c1 = param1;

end if;

end|

drop procedure if exists test|

create procedure test()
begin
select 1;
commit;
select 2;
end|

drop procedure if exists test|

?expect_exception? error : syntax error
create procedure test()
begin
select 1;
commit;
select 2;|

drop procedure if exists test|

?expect_exception? error : syntax error
create procedure test()
begin
select 1;
commit;|