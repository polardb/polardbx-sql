package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;
import java.util.function.Function;

public class ConcurrentInsertIgnoreTest extends ConcurrentDMLBaseTest {

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
    public void modifyWithInsertIgnore1() throws Exception {
        String tableName = "omc_with_insert_ignore_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s values(%d, %d + %d, 'a', 'b')", count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false);
    }

    @Test
    public void modifyWithInsertIgnore2() throws Exception {
        String tableName = "omc_with_insert_ignore_2";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint, modify column c char(10) after d";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false);
    }

    @Test
    public void modifyWithInsertIgnore4PartitionKey() throws Exception {
        String tableName = "omc_with_insert_ignore_2";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING) +
                "insert ignore into %%s values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false, true);
    }

    @Test
    public void changeWithInsertIgnore1() throws Exception {
        String tableName = "omc_with_insert_ignore_1";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "insert ignore into %%s values(%d, %d + %d, 'a', 'b')", count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false);
    }

    @Test
    public void changeWithInsertIgnore2() throws Exception {
        String tableName = "omc_with_insert_ignore_2";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false);
    }

    @Test
    public void changeMultiWithInsertIgnore1() throws Exception {
        String tableName = "omc_multi_with_insert_ignore_1";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column c d char(10), change column d c varchar(20)";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false);
    }

    @Test
    public void changeMultiWithInsertIgnore2() throws Exception {
        String tableName = "omc_multi_with_insert_ignore_2";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, drop column d, add column f char(10) default 'xyz'";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB))
                && (colC.equalsIgnoreCase(colD) || colD.equalsIgnoreCase("xyz"));
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, true);
        concurrentTestInternalWithoutGeneratedColumn(tableName, colDef, alterSql, selectSql, generator, generator,
            checker, true, false);
    }

    @Test
    public void singleChangeMultiWithInsertIgnore() throws Exception {
        String tableName = "omc_single_multi_with_insert_ignore";
        String colDef = "int unique key";
        String createSql = String.format(
            "create table %%s ("
                + "a int primary key, "
                + "b %s, "
                + "c varchar(10) default 'abc',"
                + "d varchar(10) default 'abc'"
                + ") single", colDef);
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, drop column d, add column f char(10) default 'xyz'";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB))
                && (colC.equalsIgnoreCase(colD) || colD.equalsIgnoreCase("xyz"));
        concurrentTestInternalWithCreateSql(tableName, colDef, alterSql, selectSql, generator, generator, checker,
            true, false, createSql);
    }

    @Test
    public void broadcastChangeMultiWithInsertIgnore() throws Exception {
        String tableName = "omc_single_multi_with_insert_ignore";
        String colDef = "int unique key";
        String createSql = String.format(
            "create table %%s ("
                + "a int primary key, "
                + "b %s, "
                + "c varchar(10) default 'abc',"
                + "d varchar(10) default 'abc'"
                + ") broadcast", colDef);
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, drop column d, add column f char(10) default 'xyz'";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB))
                && (colC.equalsIgnoreCase(colD) || colD.equalsIgnoreCase("xyz"));
        concurrentTestInternalWithCreateSql(tableName, colDef, alterSql, selectSql, generator, generator, checker,
            true, false, createSql);
    }

    @Test
    public void modifyWithInsertIgnore3() throws Exception {
        String tableName = "omc_with_insert_ignore_3";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,b,c,d) values(%d, %d + %d, 'a', 'b')", count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false);
    }

    @Test
    public void modifyWithInsertIgnore4() throws Exception {
        String tableName = "omc_with_insert_ignore_4";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint, modify column c char(10) after d";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,b,c,d) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false);
    }

    @Test
    public void modifyWithInsertIgnore4PartitionKey2() throws Exception {
        String tableName = "omc_with_insert_ignore_5";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s modify column b bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING) +
                "insert ignore into %%s(a,b,c,d) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true, true);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
            true);
    }

    @Test
    public void changeWithInsertIgnore3() throws Exception {
        String tableName = "omc_with_insert_ignore_3";
        String colDef = "int";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a";
        Function<Integer, String> generator = (count) -> String.format(
            "insert ignore into %%s(a,b,c,d) values(%d, %d + %d, 'a', 'b')", count, count, FILL_COUNT);
        Function<Integer, String> generator2 = (count) -> String.format(
            "insert ignore into %%s(a,e,c,d) values(%d, %d + %d, 'a', 'b')", count, count, FILL_COUNT);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator2, checker, true, true);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator2, checker, true, false);
    }

    @Test
    public void changeWithInsertIgnore4() throws Exception {
        String tableName = "omc_with_insert_ignore_4";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,b,c,d) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        Function<Integer, String> generator2 = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,e,c,d) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator2, checker, true, true);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator2, checker, true, false);
    }

    @Test
    public void changeMultiWithInsertIgnore3() throws Exception {
        String tableName = "omc_multi_with_insert_ignore_3";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, change column c d char(10), change column d c varchar(20)";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,b,c,d) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        Function<Integer, String> generator2 = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,e,d,c) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator2, checker, true, true);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator2, checker, true, false);
    }

    @Test
    public void changeMultiWithInsertIgnore4() throws Exception {
        String tableName = "omc_multi_with_insert_ignore_4";
        String colDef = "int unique key";
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
            + " alter table %s change column b e bigint, drop column d, add column f char(10) default 'xyz' after c";
        String selectSql = "select * from %s order by a desc";
        Function<Integer, String> generator = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,b,c,d) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        Function<Integer, String> generator2 = (count) -> String.format(
            buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING, ENABLE_LOCAL_UK_FULL_TABLE_SCAN) +
                "insert ignore into %%s(a,e,c,f) values(%d + %d, %d, 'a', 'b')", count, FILL_COUNT, count);
        QuadFunction<Integer, Integer, String, String, Boolean> checker =
            (colA, colB, colC, colD) -> (Objects.equals(colA, colB))
                && (colC.equalsIgnoreCase(colD) || colD.equalsIgnoreCase("xyz"));
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator2, checker, true, true);
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator2, checker, true, false);
    }
}
