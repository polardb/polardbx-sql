package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.CdcIgnore;
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
        return true;
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
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false);
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
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false);
    }

    @Test
    public void modifyWithUpsert3() throws Exception {
        String tableName = "omc_with_upsert_3";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator =
            (count) -> String.format(buildCmdExtra(ENABLE_LOCAL_UK_FULL_TABLE_SCAN)
                    + "insert into %%s values(%d + %d, %d,null,null) on duplicate key update a=a+%d",
                count, FILL_COUNT, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA - FILL_COUNT == colB;
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false);
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
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false);
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
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false);
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
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false);
    }

    @Test
    public void modifyWithUpsert4PartitionKey1() throws Exception {
        String tableName = "omc_with_upsert_1";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s values(%d, %d,null,null) on duplicate key update b=b+%d", count,
                count,
                FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA + FILL_COUNT == colB;
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false, true);
    }

    @Test
    public void modifyWithUpsert4PartitionKey2() throws Exception {
        String tableName = "omc_with_upsert_2";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator =
            (count) -> String.format("insert into %%s values(%d + %d, %d,null,null) on duplicate key update a=a+%d",
                count,
                FILL_COUNT, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA - FILL_COUNT == colB;
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false, true);
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
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, false);
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
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, false);
    }

    @Test
    public void changeWithUpsert3() throws Exception {
        String tableName = "omc_with_upsert_3";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator1 =
            (count) -> String.format(buildCmdExtra(ENABLE_LOCAL_UK_FULL_TABLE_SCAN)
                    + "insert into %%s values(%d + %d, %d,null,null) on duplicate key update a=a+%d",
                count, FILL_COUNT, count, FILL_COUNT);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s values(%d + %d, %d,null,null) on duplicate key update a=a+%d",
                count, FILL_COUNT, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA - FILL_COUNT == colB;
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, false);
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
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, false);
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
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, false);
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
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, false);
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
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator1, generator2,
            checker, true, false);
    }

    @Test
    public void singleChangeWithUpsert() throws Exception {
        String tableName = "omc_single_with_upsert";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default 4, change column c f varchar(7) default 'aaa', drop column d, add column g char(10) not null default 'xyz'";
        String selectSql = "select * from %s order by a";
        String createSql = String.format(
            "create table %%s ("
                + "a int primary key, "
                + "b %s, "
                + "c varchar(10) default 'abc',"
                + "d varchar(10) default 'abc'"
                + ") single",
            colDef);
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
        concurrentTestInternalWithCreateSql(tableName, colDef, alterSql, selectSql, generator1, generator2, checker,
            true, false, createSql);
    }

    @Test
    public void broadcastChangeWithUpsert() throws Exception {
        String tableName = "omc_broadcast_with_upsert";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default 4, change column c f varchar(7) default 'aaa', drop column d, add column g char(10) not null default 'xyz'";
        String selectSql = "select * from %s order by a";
        String createSql = String.format(
            "create table %%s ("
                + "a int primary key, "
                + "b %s, "
                + "c varchar(10) default 'abc',"
                + "d varchar(10) default 'abc'"
                + ") single",
            colDef);
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
        concurrentTestInternalWithCreateSql(tableName, colDef, alterSql, selectSql, generator1, generator2, checker,
            true, false, createSql);
    }

    @Test
    @CdcIgnore(ignoreReason = "omc 精度变更导致cdc数据校验无法通过")
    public void changeWithUpsert8() throws Exception {
        String tableName = "omc_with_update_8";
        String colDef = "float(8,2)";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e decimal(9,3)";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator1 =
            (count) -> String.format("insert into %%s values(%d, %f,null,null) on duplicate key update a=a+%d", count,
                count / 7.0, FILL_COUNT);
        Function<Integer, String> generator2 =
            (count) -> String.format("insert into %%s values(%d, %f,null,null) on duplicate key update a=a+%d", count,
                count / 7.0, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker = (colA, colB, colC, colD) -> true;

        concurrentTestInternalWithNotStrict(tableName, colDef, alterSql, selectSql, generator1, generator2, checker,
            true, true);
        concurrentTestInternalWithNotStrict(tableName, colDef, alterSql, selectSql, generator1, generator2, checker,
            true, false);
    }
}
