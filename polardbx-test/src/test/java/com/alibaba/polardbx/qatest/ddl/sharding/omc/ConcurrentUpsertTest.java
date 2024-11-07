package com.alibaba.polardbx.qatest.ddl.sharding.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.ddl.sharding.omc.ConcurrentDMLBaseTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Function;

public class ConcurrentUpsertTest extends ConcurrentDMLBaseTest {

    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());
    private final boolean isRDS80 = StorageInfoManager.checkRDS80(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType && !isRDS80);
    }

    @Override
    public boolean usingNewPartDb() {
        return false;
    }

    @Test
    public void modifyWithUpsert1() throws Exception {
        String tableName = "omc_with_upsert_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint, modify column c longtext";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s values(%d, %d,null,null) on duplicate key update b=b+%d,c = b",
                count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + FILL_COUNT == colB) && Float.parseFloat(colC) == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithUpsert2() throws Exception {
        String tableName = "omc_with_upsert_2";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint, modify column d char(20) character set gbk";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s values(%d, %d,null,null) on duplicate key update a=a+%d,d = a",
                count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA - FILL_COUNT == colB) && Float.parseFloat(colD) == colA;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithUpsert3() throws Exception {
        String tableName = "omc_with_upsert_3";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s values(%d + %d, %d,null,null) on duplicate key update a=a+%d",
                count, FILL_COUNT, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA - FILL_COUNT == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithUpsert4() throws Exception {
        String tableName = "omc_with_upsert_4";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b char(20)";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s values(%d, %d + %d,null,null) on duplicate key update b=b+%d",
                count, count, FILL_COUNT, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA + FILL_COUNT == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithUpsert5() throws Exception {
        String tableName = "omc_with_upsert_5";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint default 4, modify column c varchar(20) default 'xyz'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator =
            (count) -> String.format(
                "insert into %%s values(%d, %d + %d,null,null) on duplicate key update b=default,c=default",
                count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 4) && (colC.equalsIgnoreCase("abc")
                || colC.equalsIgnoreCase("xyz"));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithUpsert6() throws Exception {
        String tableName = "omc_with_upsert_6";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint default 4, modify column d char(10) default 'xyz'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator =
            (count) -> String.format(
                "insert into %%s values(%d, %d + %d,null,null) on duplicate key update b=default(b),c=default(d)",
                count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 4) && Float.parseFloat(colD) == colA
                && (colC.equalsIgnoreCase("abc") || colC.equalsIgnoreCase("xyz"));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpsert1() throws Exception {
        String tableName = "omc_with_upsert_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column c f char(10)";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s values(%d, %d,null,null) on duplicate key update b=b+%d,c=b",
                count, count, FILL_COUNT);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s values(%d, %d,null,null) on duplicate key update e=e+%d,f=e",
                count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + FILL_COUNT == colB) && (Float.parseFloat(colC) == colB);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpsert2() throws Exception {
        String tableName = "omc_with_upsert_2";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s values(%d, %d,null,null) on duplicate key update a=a+%d", count,
                count, FILL_COUNT);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s values(%d, %d,null,null) on duplicate key update a=a+%d", count,
                count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA - FILL_COUNT == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpsert3() throws Exception {
        String tableName = "omc_with_upsert_3";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s values(%d + %d, %d,null,null) on duplicate key update a=a+%d",
                count, FILL_COUNT, count, FILL_COUNT);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s values(%d + %d, %d,null,null) on duplicate key update a=a+%d",
                count, FILL_COUNT, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA - FILL_COUNT == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpsert4() throws Exception {
        String tableName = "omc_with_upsert_4";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s values(%d, %d + %d,null,null) on duplicate key update b=b+%d",
                count, count, FILL_COUNT, FILL_COUNT);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s values(%d, %d + %d,null,null) on duplicate key update e=e+%d",
                count, count, FILL_COUNT, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA + FILL_COUNT == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpsert5() throws Exception {
        String tableName = "omc_with_upsert_5";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default 4, change column c f char(10) default 'xyz'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format(
                "insert into %%s values(%d, %d + %d,null,null) on duplicate key update b=default,c=default",
                count, count, FILL_COUNT);
        Function<Integer, String> generator2 =
            (count) -> String.format(
                "insert into %%s values(%d, %d + %d,null,null) on duplicate key update e=default,f=default",
                count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 4)
                && (colC.equalsIgnoreCase("abc") || colC.equalsIgnoreCase("xyz"));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpsert6() throws Exception {
        String tableName = "omc_with_upsert_6";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default 4, change column c f varchar(7) default 'aaa', change column d g int default '123'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format(
                "insert into %%s values(%d, %d + %d,null,null) on duplicate key update b=default(b),c=default(d)",
                count, count, FILL_COUNT);
        Function<Integer, String> generator2 =
            (count) -> String.format(
                "insert into %%s values(%d, %d + %d,null,null) on duplicate key update e=default(e),f=default(g)",
                count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 4)
                && (colC.equalsIgnoreCase("abc") || colC.equalsIgnoreCase("123"));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }

    @Test
    public void changeWithUpsert7() throws Exception {
        String tableName = "omc_with_upsert_7";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default 4, change column c f varchar(7) default 'aaa', drop column d, add column g char(10) not null default 'xyz'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format(
                "insert into %%s values(%d, %d + %d,null,null) on duplicate key update b=default(b),c=default(d)",
                count, count, FILL_COUNT);
        Function<Integer, String> generator2 =
            (count) -> String.format(
                "insert into %%s values(%d, %d + %d,null,default) on duplicate key update e=default(e),f=default(g)",
                count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 4)
                && (colC.equalsIgnoreCase("abc") || colC.equalsIgnoreCase("xyz"));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
            1);
    }
}