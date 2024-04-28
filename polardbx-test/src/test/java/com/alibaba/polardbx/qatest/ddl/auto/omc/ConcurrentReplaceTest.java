package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import org.junit.Before;
import org.junit.Test;

import java.util.function.BiFunction;
import java.util.function.Function;

public class ConcurrentReplaceTest extends ConcurrentDMLBaseTest {

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
    public void modifyWithReplace1() throws Exception {
        String tableName = "omc_with_replace_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING)
                + "replace into %%s values(%d, %d + %d,null,null)", count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA + FILL_COUNT == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithReplace2() throws Exception {
        String tableName = "omc_with_replace_2";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING)
                + "replace into %%s values(%d + %d, %d,null,null)",
            count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA - FILL_COUNT == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithReplace3() throws Exception {
        String tableName = "omc_with_replace_3";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint default 4";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING) + "replace into %%s(a) values(%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colB == 3 || colB == 4;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyMultiWithReplace1() throws Exception {
        String tableName = "omc_multi_with_replace_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint, modify column c varchar(20), drop column d, add column e varchar(10) default 'dbc'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING)
                + "replace into %%s values(%d, %d + %d, %d, %d+%d)",
            count, count, FILL_COUNT, count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + FILL_COUNT == colB && (colD.equalsIgnoreCase("dbc")
                || Float.parseFloat(colC) + FILL_COUNT == Float.parseFloat(colD)));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyMultiWithReplace2() throws Exception {
        String tableName = "omc_multi_with_replace_2";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column c char(10) default 'dbc'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING)
                + "replace into %%s(a,b) values(%d, %d + %d)",
            count, count, FILL_COUNT, count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + FILL_COUNT == colB && (colC.equalsIgnoreCase("dbc")
                || colC.equalsIgnoreCase("abc")));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void modifyWithReplace4PartitionKey2() throws Exception {
        String tableName = "omc_with_replace_2";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING)
                + "replace into %%s values(%d + %d, %d,null,null)",
            count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA - FILL_COUNT == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1, true);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1, true);
    }

    @Test
    public void changeWithReplace1() throws Exception {
        String tableName = "omc_with_replace_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING)
                + "replace into %%s values(%d, %d + %d,null,null)",
            count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA + FILL_COUNT == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void changeWithReplace2() throws Exception {
        String tableName = "omc_with_replace_2";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING)
                + "replace into %%s values(%d + %d, %d,null,null)",
            count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colA - FILL_COUNT == colB;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void changeWithReplace3() throws Exception {
        String tableName = "omc_with_replace_3";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default 4";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING) + "replace into %%s(a) values(%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> colB == 3 || colB == 4;
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void changeMultiWithReplace1() throws Exception {
        String tableName = "omc_with_replace_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column c cc char(10) character set utf8, modify column d varchar(30) not null";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING)
                + "replace into %%s values(%d, %d + %d, %d, %d + %d)",
            count, count, FILL_COUNT, count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA + FILL_COUNT == colB
                && Float.parseFloat(colC) + FILL_COUNT == Float.parseFloat(colD));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void changeMultiWithReplace2() throws Exception {
        String tableName = "omc_with_replace_2";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column c cc char(10) character set utf8, drop column d, add column f varchar(10) default \"def d\"";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING)
                + "replace into %%s values(%d + %d, %d, \"def c\", \"def d\")",
            count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colA - FILL_COUNT == colB) && colC.equalsIgnoreCase("def c")
                && colD.equalsIgnoreCase("def d");
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }

    @Test
    public void changeMultiWithReplace3() throws Exception {
        String tableName = "omc_with_replace_3";
        String colDef = "int default 3";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint default 4, change column c f char(10) character set utf8mb4 default 'wumu'";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING) + "replace into %%s(a) values(%d)", count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (colB == 3 || colB == 4)
                && (colC.equalsIgnoreCase("abc") || colC.equalsIgnoreCase("wumu"));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
            1);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            1);
    }
}
