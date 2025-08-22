package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConcurrentDMLBaseTest extends DDLBaseNewDBTestCase {

    protected static final String USE_OMC_ALGORITHM = " ALGORITHM=OMC";
    protected static final String OMC_FORCE_TYPE_CONVERSION = "OMC_FORCE_TYPE_CONVERSION=TRUE";
    // Use logical execution since result may be different from pushdown execution
    protected static final String USE_LOGICAL_EXECUTION = "DML_EXECUTION_STRATEGY=LOGICAL";
    protected static final String DISABLE_DML_RETURNING = "DML_USE_RETURNING=FALSE";
    protected static final String ENABLE_LOCAL_UK_FULL_TABLE_SCAN =
        DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN + "=TRUE";
    protected static final int FILL_COUNT = 1500;
    protected static final int FILL_BATCH_SIZE = 1500;

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
                + "d int, "
                + "e int, "
                + "f int"
                + ") partition by hash(`c`) partitions 7",
            selectTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < FILL_COUNT; i++) {
            String sql = String.format("insert into table %s values (%d, %d, %d, %d)", selectTableName, i, i, i, i);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }

    public void concurrentTestInternal(String tableName, String colDef, String alterSql, String selectSql,
                                       Function<Integer, String> generator1, Function<Integer, String> generator2,
                                       QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                       boolean fillData, boolean withGsi) throws Exception {
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, fillData,
            withGsi, false);
    }

    public void concurrentTestInternal(String tableName, String colDef, String alterSql, String selectSql,
                                       Function<Integer, String> generator1, Function<Integer, String> generator2,
                                       QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                       boolean fillData, boolean withGsi, boolean isModifyPartitionKey)
        throws Exception {
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, fillData,
            withGsi, isModifyPartitionKey, true, null, true, !isModifyPartitionKey);
    }

    public void concurrentTestInternalWithPhyDdl(String tableName, String colDef, String alterSql, String selectSql,
                                                 Function<Integer, String> generator1,
                                                 Function<Integer, String> generator2,
                                                 QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                                 boolean fillData, boolean withGsi) throws Exception {
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, fillData,
            withGsi, false, true, null, false, false);
    }

    public void concurrentTestInternalWithoutGeneratedColumn(String tableName, String colDef, String alterSql,
                                                             String selectSql,
                                                             Function<Integer, String> generator1,
                                                             Function<Integer, String> generator2,
                                                             QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                                             boolean fillData, boolean withGsi)
        throws Exception {
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, fillData,
            withGsi, false, true, null, false, true);
    }

    public void concurrentTestInternalWithoutGeneratedColumn(String tableName, String colDef, String alterSql,
                                                             String selectSql,
                                                             Function<Integer, String> generator1,
                                                             Function<Integer, String> generator2,
                                                             QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                                             boolean fillData, boolean withGsi,
                                                             boolean isModifyPartitionKey)
        throws Exception {
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, fillData,
            withGsi, isModifyPartitionKey, true, null, false, !isModifyPartitionKey);
    }

    public void concurrentTestInternalWithNotStrict(String tableName, String colDef, String alterSql, String selectSql,
                                                    Function<Integer, String> generator1,
                                                    Function<Integer, String> generator2,
                                                    QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                                    boolean fillData, boolean withGsi)
        throws Exception {
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, fillData,
            withGsi, false, false, null, false, true);
    }

    public void concurrentTestInternalWithCreateSql(String tableName, String colDef, String alterSql, String selectSql,
                                                    Function<Integer, String> generator1,
                                                    Function<Integer, String> generator2,
                                                    QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                                    boolean fillData, boolean withGsi, String createSql)
        throws Exception {
        concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, fillData,
            withGsi, false, true, createSql, false, true);
    }

    public void concurrentTestInternal(String tableName, String colDef, String alterSql, String selectSql,
                                       Function<Integer, String> generator1, Function<Integer, String> generator2,
                                       QuadFunction<Integer, Integer, String, String, Boolean> checker,
                                       boolean fillData, boolean withGsi, boolean isModifyPartitionKey,
                                       boolean isStrictMode, String createTableSql, boolean hasGeneratedColumns,
                                       boolean isOmc)
        throws Exception {
        tableName = tableName + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String indexName1 = tableName + "_idx_1";
        String indexName2 = tableName + "_idx_2";
        String indexName3 = tableName + "_idx_3";
        Connection conn = getPolardbxConnection();
        String finalTableName = tableName;

        if (!isStrictMode) {
            String sql = "SET session sql_mode = ''";
            JdbcUtil.updateDataTddl(conn, sql, null);
        }

        try {
            String createSql;
            if (isModifyPartitionKey) {
                createSql = withGsi ? String.format(
                    "create table %s ("
                        + "a int, "
                        + "b %s, "
                        + "c varchar(10) default 'abc',"
                        + "d varchar(10) default 'abc',"
                        + "global index %s(`b`) covering(a,c,d) partition by hash(`b`) partitions 3"
                        + ") partition by hash(`b`) partitions 3",
                    tableName, colDef, indexName1) :
                    String.format(
                        "create table %s ("
                            + "a int, "
                            + "b %s, "
                            + "c varchar(10) default 'abc',"
                            + "d varchar(10) default 'abc'"
                            + ") partition by hash(`b`) partitions 3",
                        tableName, colDef);
            } else {
                createSql = withGsi ? String.format(
                    "create table %s ("
                        + "a int primary key, "
                        + "b %s, "
                        + "c varchar(10) default 'abc',"
                        + "d varchar(10) default 'abc',"
                        + "global index %s(`a`) partition by hash(`a`) partitions 3,"
                        + "global index %s(`a`) covering(b, c, d) partition by hash(`a`) partitions 3,"
                        + "clustered index %s(`a`) partition by hash(`a`) partitions 3"
                        + ") partition by hash(`a`) partitions 3",
                    tableName, colDef, indexName1, indexName2, indexName3) :
                    String.format(
                        "create table %s ("
                            + "a int primary key, "
                            + "b %s,"
                            + "c varchar(10) default 'abc',"
                            + "d varchar(10) default 'abc'"
                            + ") partition by hash(`a`) partitions 3",
                        tableName, colDef);
            }
            if (createTableSql != null) {
                createSql = String.format(createTableSql, finalTableName);
            }

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

            if (hasGeneratedColumns) {
                String alterGeneratedColumnSql = String.format(
                    "alter table %s add column tmp timestamp default current_timestamp", finalTableName);
                JdbcUtil.executeUpdateSuccess(conn, alterGeneratedColumnSql);

                alterGeneratedColumnSql = String.format(
                    "alter table %s add column v_a bigint GENERATED ALWAYS AS (tmp) virtual", finalTableName);
                JdbcUtil.executeUpdateSuccess(conn, alterGeneratedColumnSql);

                alterGeneratedColumnSql = String.format(
                    "alter table %s add column v_b bigint GENERATED ALWAYS AS (tmp) stored", finalTableName);
                JdbcUtil.executeUpdateSuccess(conn, alterGeneratedColumnSql);

                alterGeneratedColumnSql = String.format(
                    "alter table %s add column v_c bigint GENERATED ALWAYS AS (tmp) logical", finalTableName);
                JdbcUtil.executeUpdateSuccess(conn, alterGeneratedColumnSql);
            }

            if (fillData) {
                final String insert = String.format("insert into %s(a,b,c,d) values (?,?,?,?)", tableName);
                for (int i = 0; i < FILL_COUNT; i += FILL_BATCH_SIZE) {
                    List<List<Object>> params = new ArrayList<>();
                    for (int j = 0; j < FILL_BATCH_SIZE; j++) {
                        List<Object> param = new ArrayList<>();
                        param.add(j + i);
                        param.add(j + i);
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
                            if (!isStrictMode) {
                                String sql = "SET session sql_mode = ''";
                                JdbcUtil.updateDataTddl(connection, sql, null);
                            }
                            Function<Integer, String> generator = generator1;
                            boolean changed = false;
                            while ((!shouldStop.get() || fillData) && totalCount.get() < FILL_COUNT) {
                                String sql = String.format(generator.apply(totalCount.get()), finalTableName);
                                try {
                                    JdbcUtil.executeUpdateSuccess(connection, sql);
                                } catch (AssertionError e) {
                                    if (e.getMessage().contains("Lock wait timeout exceeded") || e.getMessage()
                                        .contains("Deadlock found")) {
                                        // ignore
                                        Thread.sleep(500);
                                        totalCount.getAndDecrement();
                                    } else if ((e.getMessage().contains("Unknown target column") || e.getMessage()
                                        .contains("not found")) && !changed) {
                                        changed = true;
                                        generator = generator2;
                                        totalCount.getAndDecrement();
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
                        if (!isStrictMode) {
                            String sql = "SET session sql_mode = ''";
                            JdbcUtil.updateDataTddl(connection, sql, null);
                        }
                        Thread.sleep(1000);
                        String algorithm = isOmc ? USE_OMC_ALGORITHM : "";
                        String batchHint = "/*+TDDL:cmd_extra(GSI_BACKFILL_BATCH_SIZE=50,CHANGE_SET_APPLY_BATCH=32)*/";
                        String sql = batchHint + String.format(alterSql, finalTableName) + algorithm;
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
                if (!isModifyPartitionKey) {
                    index.add(String.format(" force index (%s) ", indexName2));
                    index.add(String.format(" force index (%s) ", indexName3));
                }
            }

            BiFunction<AtomicBoolean, AtomicInteger, Void> selectFunc =
                new BiFunction<AtomicBoolean, AtomicInteger, Void>() {
                    @SneakyThrows
                    @Override
                    public Void apply(AtomicBoolean shouldStop, AtomicInteger totalCount) {
                        Connection connection = getPolardbxConnection();
                        if (!isStrictMode) {
                            String sql = "SET session sql_mode = ''";
                            JdbcUtil.updateDataTddl(connection, sql, null);
                        }
                        Statement statement = connection.createStatement();
                        try {
                            int cnt = 0;
                            while ((!shouldStop.get() || fillData) && totalCount.get() < FILL_COUNT) {
                                int tot = totalCount.get();
                                ResultSet rs = statement.executeQuery(
                                    String.format(selectSql, finalTableName + index.get(cnt % index.size())));
                                cnt++;
                                while (rs.next() && tot > 0) {
                                    int colA = rs.getInt(1);
                                    Object tmpB = rs.getObject(2);
                                    int colB = 0;
                                    if (tmpB instanceof Integer) {
                                        colB = (Integer) tmpB;
                                    } else if (tmpB instanceof Long) {
                                        colB = ((Long) tmpB).intValue();
                                    } else if (tmpB instanceof String) {
                                        colB = (int) Float.parseFloat((String) tmpB);
                                    }
                                    String colC = rs.getObject(3) == null ? null : rs.getObject(3).toString();
                                    String colD = rs.getObject(4) == null ? null : rs.getObject(4).toString();
                                    if (!checker.apply(colA, colB, colC, colD)) {
                                        System.out.println(tot + ": " + colA + " " + colB + " " + colC + " " + colD);
                                    }
                                    assertTrue(checker.apply(colA, colB, colC, colD));
                                    tot--;
                                }
                                rs.close();
                                Thread.sleep(500);
                            }
                            statement.close();

                            Thread.sleep(500); // sleep to wait dmlTask
                            ResultSet rs =
                                JdbcUtil.executeQuerySuccess(connection, String.format(selectSql, finalTableName));
                            int actualCount = 0;
                            while (rs.next()) {
                                int colA = rs.getInt(1);
                                Object tmpB = rs.getObject(2);
                                int colB = 0;
                                if (tmpB instanceof Integer) {
                                    colB = (Integer) tmpB;
                                } else if (tmpB instanceof Long) {
                                    colB = ((Long) tmpB).intValue();
                                } else if (tmpB instanceof String) {
                                    colB = (int) Float.parseFloat((String) tmpB);
                                }
                                String colC = rs.getObject(3) == null ? null : rs.getObject(3).toString();
                                String colD = rs.getObject(4) == null ? null : rs.getObject(4).toString();
                                if (!checker.apply(colA, colB, colC, colD)) {
                                    System.out.println(actualCount + ": " + colA + " " + colB);
                                }
                                assertTrue(checker.apply(colA, colB, colC, colD));
                                actualCount++;
                            }
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
                if (!isModifyPartitionKey) {
                    checkGsi(conn, getRealGsiName(conn, tableName, indexName2));
                    checkGsi(conn, getRealGsiName(conn, tableName, indexName3));
                }
            }
        } finally {
            conn.close();
        }
    }

    @FunctionalInterface
    public interface QuadFunction<T, U, V, W, R> {
        R apply(T t, U u, V v, W w);
    }
}
