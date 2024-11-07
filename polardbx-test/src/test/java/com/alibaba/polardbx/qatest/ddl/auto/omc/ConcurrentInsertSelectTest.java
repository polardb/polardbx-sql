package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ConcurrentInsertSelectTest extends ConcurrentDMLBaseTest {
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
    public void modifyWithInsertSelect() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        String selectTableName = "omc_with_insert_select_src_tbl";
        buildSelectTable(selectTableName);

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) select %d,%d+1", count, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (colA == colB - 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsertSelect 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_2";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s modify column b bigint, modify column d text";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(b,a,d,c) select c,d+1,e,f+1 from %s where c=%d",
                    selectTableName, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (colA == colB + 1) && (Float.parseFloat(colC)
                    == Float.parseFloat(colD) + 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsertSelect 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_3";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s modify column b bigint, modify column d char(55)";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s select * from %s where c=%d", selectTableName, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (Objects.equals(colA, colB) && colC.equalsIgnoreCase(colD));
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsertSelect 3");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void modifyWithInsertSelect4PartitionKey() throws Exception {
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final List<Callable<Void>> tasks = new ArrayList<>();

        String selectTableName = "omc_with_insert_select_src_tbl";
        buildSelectTable(selectTableName);

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) select %d,%d+1", count, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (colA == colB - 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1, true);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1, true);
            System.out.println("modifyWithInsertSelect 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_2";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) select %d,%d+1", count, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (colA == colB - 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1, true);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1, true);
            System.out.println("modifyWithInsertSelect 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_3";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(b,a) select c,d+1 from %s where c=%d", selectTableName,
                    count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (colA == colB + 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1, true);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1, true);
            System.out.println("modifyWithInsertSelect 3");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_4";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s select * from %s where c=%d", selectTableName, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (Objects.equals(colA, colB));
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1, true);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1, true);
            System.out.println("modifyWithInsertSelect 4");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void changeWithInsertSelect() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        String selectTableName = "omc_with_insert_select_src_tbl";
        buildSelectTable(selectTableName);

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s change column b e bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s(a,b) select %d,%d+1", count, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s(a,e) select %d,%d+1", count, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (colA == colB - 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("modifyWithInsertSelect 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_2";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s change column b e bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s(b,a) select c,d+1 from %s where c=%d", selectTableName,
                    count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s(e,a) select c,d+1 from %s where c=%d", selectTableName,
                    count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (colA == colB + 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("modifyWithInsertSelect 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_3";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s select * from %s where c=%d", selectTableName, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s select * from %s where c=%d", selectTableName, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (Objects.equals(colA, colB));
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("modifyWithInsertSelect 3");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void changeMultiWithInsertSelect() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        String selectTableName = "omc_multi_with_is_src_tb";
        buildSelectTable(selectTableName);

        tasks.add(() -> {
            String tableName = "omc_multi_with_insert_select_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s change column b e bigint, change column c f text, change d g char(10)";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s(a,b,c,d) select %d,%d+1,%d,%d+1", count, count, count, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s(a,e,f,g) select %d,%d+1,%d,%d+1", count, count, count, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (colA == colB - 1)
                    && (Float.parseFloat(colC) == Float.parseFloat(colD) - 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("modifyMultiWithInsertSelect 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_multi_with_insert_select_2";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s change column b e bigint, change column c d char(10), change column d c char(10)";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s(b,a,c,d) select c,d+1,e,f+1 from %s where c=%d",
                    selectTableName, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s(e,a,d,c) select c,d+1,e,f+1 from %s where c=%d",
                    selectTableName, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (colA == colB + 1)
                    && (Float.parseFloat(colC) + 1 == Float.parseFloat(colD));
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("modifyMultiWithInsertSelect 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_multi_with_insert_select_3";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s modify column b bigint, change column c f text, change d g char(10)";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s select * from %s where c=%d", selectTableName, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s select * from %s where c=%d", selectTableName, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (Objects.equals(colA, colB)) && (colC.equalsIgnoreCase(colD));
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("modifyMultiWithInsertSelect 3");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void singleBroadcastChangeMultiWithInsertSelect() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        String selectTableName = "omc_sb_multi_with_is_src_tb";
        buildSelectTable(selectTableName);

        tasks.add(() -> {
            String tableName = "omc_multi_with_insert_select_1";
            String colDef = "int";
            String createSql = String.format(
                "create table %%s ("
                    + "a int primary key, "
                    + "b %s, "
                    + "c varchar(10) default 'abc',"
                    + "d varchar(10) default 'abc'"
                    + ") single", colDef);
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s change column b e bigint, change column c f text, change d g char(10)";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s(a,b,c,d) select %d,%d+1,%d,%d+1", count, count, count, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s(a,e,f,g) select %d,%d+1,%d,%d+1", count, count, count, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (colA == colB - 1)
                    && (Float.parseFloat(colC) == Float.parseFloat(colD) - 1);
            concurrentTestInternalWithCreateSql(tableName, colDef, alterSql, selectSql, generator1, generator2, checker,
                false, false, 1, createSql);
            System.out.println("modifyMultiWithInsertSelect single");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_multi_with_insert_select_2";
            String colDef = "int";
            String createSql = String.format(
                "create table %%s ("
                    + "a int primary key, "
                    + "b %s, "
                    + "c varchar(10) default 'abc',"
                    + "d varchar(10) default 'abc'"
                    + ") broadcast", colDef);
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION)
                + " alter table %s change column b e bigint, change column c d char(10), change column d c char(10)";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s(b,a,c,d) select c,d+1,e,f+1 from %s where c=%d",
                    selectTableName, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s(e,a,d,c) select c,d+1,e,f+1 from %s where c=%d",
                    selectTableName, count);
            QuadFunction<Integer, Integer, String, String, Boolean> checker =
                (colA, colB, colC, colD) -> (colA == colB + 1)
                    && (Float.parseFloat(colC) + 1 == Float.parseFloat(colD));
            concurrentTestInternalWithCreateSql(tableName, colDef, alterSql, selectSql, generator1, generator2, checker,
                false, false, 1, createSql);
            System.out.println("modifyMultiWithInsertSelect broadcast");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }
}
