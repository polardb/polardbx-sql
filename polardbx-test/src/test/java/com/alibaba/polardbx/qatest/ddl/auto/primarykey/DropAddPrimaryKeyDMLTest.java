package com.alibaba.polardbx.qatest.ddl.auto.primarykey;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import lombok.SneakyThrows;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DropAddPrimaryKeyDMLTest extends DDLBaseNewDBTestCase {

    private static final int FILL_COUNT = 2000;
    private static final int FILL_BATCH_SIZE = 2000;
    private static final String USE_LOGICAL_EXECUTION = "DML_EXECUTION_STRATEGY=LOGICAL";
    private static final String DISABLE_DML_RETURNING = "DML_USE_RETURNING=FALSE";

    protected static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    public void buildSelectTable(String selectTableName) {
        dropTableIfExists(selectTableName);
        String createSql = String.format(
            "create table %s ("
                + "c int primary key, "
                + "d int "
                + ") partition by hash(`c`) partitions 7",
            selectTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < FILL_COUNT; i++) {
            String sql = String.format("insert into table %s values (%d, %d)", selectTableName, i, i);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }

    @Test
    public void modifyWithInsert4PrimaryKey() throws Exception {
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final List<Callable<Void>> tasks = new ArrayList<>();

        tasks.add(() -> {
            String tableName = "modify_pk_with_insert_1";
            String colDef = "int";
            String alterSql = " alter table %s drop primary key, add primary key(b)";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) values (%d,%d+1)", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsert 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "modify_pk_with_insert_2";
            String colDef = "int";
            String alterSql = " alter table %s drop primary key, add primary key(b)";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) values (%d,%d),(%d,%d)", count * 2, count * 2 + 1,
                    count * 2 + 1, count * 2 + 2);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                2);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                2);
            System.out.println("modifyWithInsert 7");
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
    public void modifyWithInsertIgnore4PrimaryKey() throws Exception {
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final List<Callable<Void>> tasks = new ArrayList<>();

        // 2
        tasks.add(() -> {
            String tableName = "modify_pk_with_insert_ignore_1";
            String colDef = "int unique key";
            String alterSql = " alter table %s drop primary key, add primary key(b)";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator =
                (count) -> String.format("insert ignore into %%s values(%d, %d)", count, count);
            BiFunction<Integer, Integer, Boolean> checker = Objects::equals;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithInsertIgnore 1");
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
    public void modifyWithUpdate4PrimaryKey() throws Exception {
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final List<Callable<Void>> tasks = new ArrayList<>();

        tasks.add(() -> {
            String tableName = "modify_pk_with_update_1";
            String colDef = "int";
            String alterSql = " alter table %s drop primary key, add primary key(b)";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set b=%d + %d where a=%d", count, FILL_COUNT, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpdate 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "modify_pk_with_update_2";
            String colDef = "int";
            String alterSql = " alter table %s drop primary key, add primary key(a, b)";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set b=%d+%d where a=%d", count, FILL_COUNT, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpdate 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "modify_pk_with_update_3";
            String colDef = "int ";
            String alterSql = " alter table %s drop primary key, add primary key(a)";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator = (count) -> String.format("update %%s set b=a+1 where a=%d", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == colA + 1;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpdate 3");
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
    public void modifyWithInsertSelect4PrimaryKey() throws Exception {
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final List<Callable<Void>> tasks = new ArrayList<>();

        String selectTableName = "modify_pk_with_insert_select_src_tbl";
        buildSelectTable(selectTableName);

        tasks.add(() -> {
            String tableName = "modify_pk_with_insert_select_1";
            String colDef = "int";
            String alterSql = " alter table %s drop primary key, add primary key(b)";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) select %d,%d+1", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> (colA == colB - 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsertSelect 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "modify_pk_with_insert_select_2";
            String colDef = "int";
            String alterSql = "alter table %s drop primary key, add primary key(a, b)";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) select %d,%d+1", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> (colA == colB - 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsertSelect 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "modify_pk_with_insert_select_3";
            String colDef = "int";
            String alterSql = " alter table %s drop primary key, add primary key(b, a)";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(b,a) select c,d+1 from %s where c=%d", selectTableName,
                    count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> (colA == colB + 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsertSelect 3");
            return null;
        });

        tasks.add(() -> {
            String tableName = "modify_pk_with_insert_select_4";
            String colDef = "int";
            String alterSql = " alter table %s drop primary key, add primary key(a)";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s select * from %s where c=%d", selectTableName, count);
            BiFunction<Integer, Integer, Boolean> checker = Integer::equals;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
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
    public void modifyWithReplace4PrimaryKey() throws Exception {
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final List<Callable<Void>> tasks = new ArrayList<>();

        tasks.add(() -> {
            String tableName = "modify_pk_with_replace_1";
            String colDef = "int";
            String alterSql = " alter table %s drop primary key, add primary key(b)";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator = (count) -> String.format(
                buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING) + "replace into %%s values(%d, %d)",
                count, count);
            BiFunction<Integer, Integer, Boolean> checker = Objects::equals;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithReplace 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "modify_pk_with_replace_2";
            String colDef = "int unique key";
            String alterSql = " alter table %s drop primary key, add primary key(a, b)";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator = (count) -> String.format(
                buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING) + "replace into %%s values(%d + %d, %d)",
                count, FILL_COUNT, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA - FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithReplace 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "modify_pk_with_replace_3";
            String colDef = "int unique key";
            String alterSql = " alter table %s drop primary key, add primary key(b, a)";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator = (count) -> String.format(
                buildCmdExtra(USE_LOGICAL_EXECUTION, DISABLE_DML_RETURNING) + "replace into %%s values(%d + %d, %d)",
                count, FILL_COUNT, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA - FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithReplace 3");
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
    public void modifyWithUpsert4PartitionKey() throws Exception {
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final List<Callable<Void>> tasks = new ArrayList<>();

        tasks.add(() -> {
            String tableName = "modify_pk_with_upsert_1";
            String colDef = "int unique key";
            String alterSql = " alter table %s drop primary key, add primary key(b)";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s values(%d, %d) on duplicate key update b=b+%d", count, count,
                    FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpsert 1");
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

    public void concurrentTestInternal(String tableName, String colDef, String alterSql, String selectSql,
                                       Function<Integer, String> generator1, Function<Integer, String> generator2,
                                       BiFunction<Integer, Integer, Boolean> checker, boolean fillData, boolean withGsi,
                                       int batchSize)
        throws Exception {
        tableName = tableName + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String indexName1 = tableName + "_idx_1";
        Connection conn = getPolardbxConnection();
        String finalTableName = tableName;

        try {
            String createSql;

            createSql = withGsi ? String.format(
                "create table %s ("
                    + "a int primary key, "
                    + "b %s, "
                    + "global index %s(`a`) covering(b) partition by hash(`a`) partitions 3"
                    + ") partition by hash(`b`) partitions 3",
                tableName, colDef, indexName1) :
                String.format(
                    "create table %s ("
                        + "a int primary key, "
                        + "b %s "
                        + ") partition by hash(`b`) partitions 3",
                    tableName, colDef);

            // Retry in case of table group not exist
            int retryCnt = 0;
            while (true) {
                try {
                    JdbcUtil.executeUpdateSuccess(conn, createSql);
                    break;
                } catch (Throwable e) {
                    retryCnt++;
                    if (retryCnt > 5) {
                        throw e;
                    }
                    System.out.println("retry " + retryCnt + " " + e.getMessage());
                }
            }

            if (fillData) {
                final String insert = String.format("insert into %s values (?,?)", tableName);
                for (int i = 0; i < FILL_COUNT; i += FILL_BATCH_SIZE) {
                    List<List<Object>> params = new ArrayList<>();
                    for (int j = 0; j < FILL_BATCH_SIZE; j++) {
                        List<Object> param = new ArrayList<>();
                        param.add(j + i);
                        param.add(j + i);
                        params.add(param);
                    }
                    JdbcUtil.updateDataBatch(conn, insert, params);
                }
            }

            BiFunction<AtomicBoolean, AtomicInteger, Void> dmlFunc =
                new BiFunction<AtomicBoolean, AtomicInteger, Void>() {
                    @SneakyThrows
                    @Override
                    public Void apply(AtomicBoolean shouldStop, AtomicInteger totalCount) {
                        Connection connection = getPolardbxConnection();
                        try {
                            Function<Integer, String> generator = generator1;
                            boolean changed = false;
                            while ((!shouldStop.get() || fillData) && totalCount.get() < FILL_COUNT) {
                                String sql = String.format(generator.apply(totalCount.get()), finalTableName);
                                try {
                                    JdbcUtil.executeUpdateSuccess(connection, sql);
                                } catch (Throwable e) {
                                    if (!changed) {
                                        changed = true;
                                        generator = generator2;
                                        totalCount.getAndDecrement();
                                    } else if (e.getMessage().contains("Lock wait timeout exceeded")) {
                                        // ignore
                                    } else {
                                        throw e;
                                    }
                                }

                                totalCount.incrementAndGet();
                            }
                        } finally {
                            connection.close();
                        }
                        return null;
                    }
                };

            BiFunction<AtomicBoolean, AtomicInteger, Void> alterFunc =
                new BiFunction<AtomicBoolean, AtomicInteger, Void>() {
                    @SneakyThrows
                    @Override
                    public Void apply(AtomicBoolean shouldStop, AtomicInteger totalCount) {
                        Connection connection = getPolardbxConnection();
                        Thread.sleep(1000);
                        String sql = String.format(alterSql, finalTableName);
                        try {
                            execDdlWithRetry(tddlDatabase1, finalTableName, sql, connection);
                        } finally {
                            shouldStop.set(true);
                            connection.close();
                        }
                        Thread.sleep(1000);
                        System.out.println(totalCount.get());
                        return null;
                    }
                };

            List<String> index = new ArrayList<>();
            index.add(" force index (primary)");
            if (withGsi) {
                index.add(String.format(" force index (%s) ", indexName1));
            }

            BiFunction<AtomicBoolean, AtomicInteger, Void> selectFunc =
                new BiFunction<AtomicBoolean, AtomicInteger, Void>() {
                    @SneakyThrows
                    @Override
                    public Void apply(AtomicBoolean shouldStop, AtomicInteger totalCount) {
                        Connection connection = getPolardbxConnection();
                        try {
                            int cnt = 0;
                            Statement stmt = connection.createStatement();
                            while ((!shouldStop.get() || fillData) && totalCount.get() < FILL_COUNT) {
                                int tot = totalCount.get();
                                ResultSet rs = stmt.executeQuery(
                                    String.format(selectSql, finalTableName + index.get(cnt % index.size())));
                                cnt++;
                                ResultSetMetaData rsmd = rs.getMetaData();
                                assertEquals(2, rsmd.getColumnCount());
                                while (rs.next() && tot > 0) {
                                    int colA = rs.getInt(1);
                                    int colB = rs.getInt(2);
                                    if (!checker.apply(colA, colB)) {
                                        System.out.println(tot + ": " + colA + " " + colB);
                                    }
                                    assertTrue(checker.apply(colA, colB));
                                    tot--;
                                }
                                rs.close();
                                Thread.sleep(500);
                            }
                            stmt.close();

                            Thread.sleep(500); // sleep to wait dmlTask
                            ResultSet rs =
                                JdbcUtil.executeQuerySuccess(connection, String.format(selectSql, finalTableName));
                            ResultSetMetaData rsmd = rs.getMetaData();
                            assertEquals(2, rsmd.getColumnCount());
                            int actualCount = 0;
                            while (rs.next()) {
                                int colA = rs.getInt(1);
                                int colB = rs.getInt(2);
                                if (!checker.apply(colA, colB)) {
                                    System.out.println(actualCount + ": " + colA + " " + colB);
                                }
                                assertTrue(checker.apply(colA, colB));
                                actualCount++;
                            }
                            // assertEquals(totalCount.get() * batchSize, actualCount); // this will fail sometimes due to sync problem
                        } finally {
                            connection.close();
                        }
                        return null;
                    }
                };

            AtomicBoolean shouldStop = new AtomicBoolean(false);
            AtomicInteger totalCount = new AtomicInteger(0);

            final ExecutorService threadPool = Executors.newFixedThreadPool(3);
            Callable<Void> dmlTask = () -> {
                dmlFunc.apply(shouldStop, totalCount);
                return null;
            };
            Callable<Void> alterTask = () -> {
                alterFunc.apply(shouldStop, totalCount);
                return null;
            };
            Callable<Void> selectTask = () -> {
                selectFunc.apply(shouldStop, totalCount);
                return null;
            };

            ArrayList<Future<Void>> results = new ArrayList<>();
            results.add(threadPool.submit(dmlTask));
            results.add(threadPool.submit(alterTask));
            results.add(threadPool.submit(selectTask));

            try {
                for (Future<Void> result : results) {
                    result.get();
                }
            } catch (Throwable e) {
                e.printStackTrace();
                throw (e);
            } finally {
                //报错需设置退出信号,防止线程泄漏
                shouldStop.set(true);
                totalCount.set(FILL_COUNT);
                threadPool.shutdown();
            }

            if (withGsi) {
                checkGsi(conn, getRealGsiName(conn, tableName, indexName1));
            }
        } finally {
            conn.close();
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
