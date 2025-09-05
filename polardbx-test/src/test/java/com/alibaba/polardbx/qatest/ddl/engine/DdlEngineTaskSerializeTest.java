package com.alibaba.polardbx.qatest.ddl.engine;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.mdl.MdlDetectionTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@NotThreadSafe
@RunWith(Parameterized.class)
public class DdlEngineTaskSerializeTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(DdlEngineTaskSerializeTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public DdlEngineTaskSerializeTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    public int smallDelay = 1;

    public int largeDelay = 5;

    public Long seq = 0L;

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}});
    }

    @Before
    public void init() {
        this.tableName = schemaPrefix + randomTableName("pushdown", 4);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private Future<Boolean> executeSqlAndCommit(ExecutorService threadPool, String tableName,
                                                Connection connection, String sql) {
        return threadPool.submit(() -> {
            try {
                log.info("execute ddl " + sql);
                JdbcUtil.executeUpdateSuccess(connection, sql);
            } catch (Exception e) {
                log.info(e.getMessage());
                return false;
            }
            return true;
        });
    }

    private Future<Boolean> checkSerializeTask(ExecutorService threadPool, String tableName,
                                               Connection connection) {

        return threadPool.submit(() -> {
            Boolean checkOk = false;
            try {
                Thread.sleep(smallDelay * 1000L);
                Boolean ddlSerrialized = false;
                String schemaName =
                    JdbcUtil.getAllResult(JdbcUtil.executeQuery("select database()", connection)).get(0).get(0)
                        .toString();
                Map<String, String> queryDdlTaskName2Value = null;
                while (!ddlSerrialized) {
                    Thread.sleep(500);
                    String queryDdlState = String.format(
                        "select job_id, state from metadb.ddl_engine where schema_name = '%s' and object_name = '%s'",
                        schemaName, tableName);
                    List<List<Object>> results =
                        JdbcUtil.getAllResult(JdbcUtil.executeQuery(queryDdlState, connection));
                    if (!results.isEmpty()) {
                        Long jobId = Long.valueOf(results.get(0).get(0).toString());
                        String queryDdlTaskValue = String.format(
                            "select name, task_id, value from metadb.ddl_engine_task where schema_name = '%s' and job_id = '%d'",
                            schemaName, jobId);
                        List<List<Object>> queryDdlTaskValueResults =
                            JdbcUtil.getAllResult(JdbcUtil.executeQuery(queryDdlTaskValue,
                                connection));
                        queryDdlTaskName2Value = queryDdlTaskValueResults.stream()
                            .collect(Collectors.toMap(o -> o.get(0).toString() + o.get(1).toString(), o -> o.get(2).toString()));
                    }
                    ddlSerrialized =
                        (!results.isEmpty()) && (!queryDdlTaskName2Value.isEmpty());
                }

                Assert.assertTrue("ddl task value not long enough", queryDdlTaskName2Value.values().stream()
                    .anyMatch(o -> o.getBytes().length > DdlHelper.COMPRESS_THRESHOLD_SIZE || DdlHelper.isGzip(o)));
                for (String taskName : queryDdlTaskName2Value.keySet()) {
                    String value = queryDdlTaskName2Value.get(taskName);
                    Boolean valueCompressed = DdlHelper.isGzip(value);
                    long valueLength = value.getBytes().length;
                    long deCompressValueLength = -1L;
                    if (valueCompressed) {
                        deCompressValueLength = DdlHelper.decompress(value).getBytes().length;
                    }
                    String info =
                        String.format("taskName:%s, value compressed, value length: %d, compressed value length: %d",
                            taskName, deCompressValueLength, valueLength);
                    if (!valueCompressed) {
                        info = String.format("taskName: %s, value not compressed, value length: %d", taskName,
                            valueLength);
                    }
                    logger.info(info);
                    Assert.assertFalse("long value not compressed! " + taskName,
                        valueLength > DdlHelper.COMPRESS_THRESHOLD_SIZE && !valueCompressed);
                    Assert.assertFalse("short value also compressed! " + taskName, valueCompressed
                        && deCompressValueLength <= DdlHelper.COMPRESS_THRESHOLD_SIZE);
                }
                Boolean completeState = false;
                while (!completeState) {
                    Thread.sleep(5000);
                    String queryDdlState = String.format(
                        "select job_id, state from metadb.ddl_engine where schema_name = '%s' and object_name = '%s'",
                        schemaName, tableName);
                    List<List<Object>> results =
                        JdbcUtil.getAllResult(JdbcUtil.executeQuery(queryDdlState, connection));
                    completeState =
                        results.isEmpty() || (results.get(0).get(1).toString().equalsIgnoreCase("COMPLETED"));
                }
                String checkTableSql = String.format("check table %s", tableName);
                logger.info("execute SQL:" + checkTableSql);
                List<List<Object>> checkTableResult =
                    JdbcUtil.getAllResult(JdbcUtil.executeQuery(checkTableSql, connection));
                logger.info("check table result: " + checkTableResult.toString());
                checkOk = checkTableResult.stream().noneMatch(o -> !(o.get(3).toString().equalsIgnoreCase("OK")));
                logger.info("check table ok? " + checkOk);
            } catch (Throwable e) {
                log.info(e.getMessage());
                return false;
            }
            return checkOk;
        });
    }

    void innerTest(String tableName, Connection connection, String sql, int maxWaitTimeout, boolean isDdl,
                   String otherErrMsg) throws SQLException {
        final ExecutorService ddlThreadPool = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
            new NamedThreadFactory(MdlDetectionTest.class.getSimpleName(), false));
        final List<Future<Boolean>> futures = new LinkedList<>();

        // Connection 1: ddl
        Connection connection1 = getPolardbxConnection();
        if(isDdl) {
            futures.add(checkSerializeTask(ddlThreadPool, tableName, connection1));
        }
        futures.add(executeSqlAndCommit(ddlThreadPool, tableName, tddlConnection, sql));

        for (Future<Boolean> future : futures) {
            try {
                if (!future.get(maxWaitTimeout, TimeUnit.SECONDS)) {
                    Assert.fail(otherErrMsg);
                }
            } catch (TimeoutException e) {
                e.printStackTrace();
                Assert.fail(otherErrMsg);
            } catch (Exception e) {
                Assert.fail("Task failed for unknown reason");
            }
        }

    }

    public String generateColumnName() {
        seq += 1;
        return String.format("very_long_column_%046d", seq);
    }

    public String generateTableName(int partNum) {
        return String.format("very_long_table_name_%040d", partNum);
    }

    public String generateColumnDef(String partitioinByColumnName, int columnNum) {
        String columnDefTemplate = "`%s` int";
        List<String> columnDefs = new ArrayList<>();
        columnDefs.add(String.format(columnDefTemplate, partitioinByColumnName));
        for (int i = 0; i < columnNum; i++) {
            columnDefs.add(String.format(columnDefTemplate, generateColumnName()));
        }
        return String.format("(%s)", StringUtil.join(",", columnDefs).toString());
    }

    public String generatePartitionByDef(String partitionByColumnName, int partitionNum) {
        return String.format(" partition by hash(`%s`) partitions %d", partitionByColumnName, partitionNum);
    }

    @Test
    public void testCreateTable1024Partitions() throws SQLException {
        String mytable = generateTableName(1024);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String partitionByColumnName = generateColumnName();
        String createTableStmt = "create table " + createOption + " %s" + generateColumnDef(partitionByColumnName, 10) +
            generatePartitionByDef(partitionByColumnName, 1024);
        String sql = String.format(createTableStmt, mytable);
        int maxWaitTimeout = 1200;
        String checkPhyTableFailedHint =
            String.format(
                "/*+TDDL:CMD_EXTRA(EMIT_PHY_TABLE_DDL_DELAY=%d, CREATE_TABLE_SKIP_CDC=true, ACQUIRE_CREATE_TABLE_GROUP_LOCK=false)*/",
                largeDelay);
        sql = checkPhyTableFailedHint + sql;

        String errMsg = "doesn't exist";
        String otherErrMsg = "Execute AlterTableCheckPhyTableDoneFailed timeout.";
        innerTest(mytable, tddlConnection, sql, maxWaitTimeout, true, otherErrMsg);
    }

    @Test
    public void testCreateTable800Partitions() throws SQLException {
        String mytable = generateTableName(800);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String partitionByColumnName = generateColumnName();
        String createTableStmt = "create table " + createOption + " %s" + generateColumnDef(partitionByColumnName, 40) +
            generatePartitionByDef(partitionByColumnName, 800);
        String sql = String.format(createTableStmt, mytable);
        int maxWaitTimeout = 1200;
        String checkPhyTableFailedHint =
            String.format(
                "/*+TDDL:CMD_EXTRA(ACQUIRE_CREATE_TABLE_GROUP_LOCK=false)*/");
        sql = checkPhyTableFailedHint + sql;

        String errMsg = "doesn't exist";
        logger.info("execute " + sql);
        String otherErrMsg = "Execute AlterTableCheckPhyTableDoneFailed timeout.";
        innerTest(mytable, tddlConnection, sql, maxWaitTimeout, true, otherErrMsg);
        logger.info("execute " + sql + " success");

        String checkTableStmt = "check table " + mytable;
        logger.info("execute " + checkTableStmt);
        innerTest(mytable, tddlConnection, checkTableStmt, maxWaitTimeout, false, otherErrMsg);
        logger.info("execute " + checkTableStmt + " success");

        String alterTableAddColumnStmt = "alter table " + mytable + " add column x1 int";
        logger.info("execute " + alterTableAddColumnStmt);
        innerTest(mytable, tddlConnection, alterTableAddColumnStmt, maxWaitTimeout, true, otherErrMsg);
        logger.info("execute " + alterTableAddColumnStmt + " success");

        String analyzeTableStmt = "analyze table " + mytable;
        logger.info("execute " + analyzeTableStmt);
        JdbcUtil.executeQuerySuccess(tddlConnection, analyzeTableStmt);
        logger.info("execute " + analyzeTableStmt + " success");
    }
}
