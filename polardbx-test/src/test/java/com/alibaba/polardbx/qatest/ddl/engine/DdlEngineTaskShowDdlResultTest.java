package com.alibaba.polardbx.qatest.ddl.engine;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.mdl.MdlDetectionTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.repo.mysql.handler.ddl.newengine.DdlEngineShowResultsHandler;
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
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
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
public class DdlEngineTaskShowDdlResultTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(DdlEngineTaskShowDdlResultTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public DdlEngineTaskShowDdlResultTest(boolean crossSchema) {
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
        this.tableName = schemaPrefix + randomTableName("showddl", 4);
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

    void innerTest(String tableName, Connection connection, String sql, int maxWaitTimeout,
                   String otherErrMsg) throws SQLException, InterruptedException {
        final ExecutorService ddlThreadPool = new ThreadPoolExecutor(1, 1, 0L,
            TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
            new NamedThreadFactory(MdlDetectionTest.class.getSimpleName(), false));

        // Connection 1: ddl
        Connection connection1 = getPolardbxConnection();
        String estimatedStartTime = DdlEngineShowResultsHandler.convertTimeMillisToDate(System.currentTimeMillis());
        String estimatedEndTime;
        final Future<Boolean> future = executeSqlAndCommit(ddlThreadPool, tableName, tddlConnection, sql);

        // JOB_ID, SCHEMA_NAME, OBJECT_NAME, DDL_TYPE, RESULT_TYPE, RESULT_CONTENT, DDL_STMT, START_TIME, END_TIME
        Boolean flag = false;
        for (int i = 0; i < maxWaitTimeout && !flag; i++) {
            Thread.sleep(1000);
            JdbcUtil.executeQuerySuccess(connection1, " set global MAX_SHOW_DDL_RESULT_STMT_LENGTH=10240");
            List<List<Object>> results =
                JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(connection1, "show ddl result"));
            for (List<Object> result : results) {
                if (result.get(1).toString().equalsIgnoreCase(getDdlSchema()) && result.get(6).toString()
                    .equalsIgnoreCase(sql)) {
                    if (result.get(4).toString().equalsIgnoreCase("RUNNING")) {
                        String startTime = result.get(7).toString();
                        if (!checkIfCloseTimeStamp(estimatedStartTime, startTime, 10L)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                                String.format("expected start time %s, but get %s in show ddl ", estimatedStartTime,
                                    startTime));
                        }
                    } else if (result.get(4).toString().equalsIgnoreCase("SUCCESS")) {
                        String startTime = result.get(7).toString();
                        String endTime = result.get(8).toString();
                        estimatedEndTime =
                            DdlEngineShowResultsHandler.convertTimeMillisToDate(System.currentTimeMillis());
                        if (!checkIfCloseTimeStamp(estimatedStartTime, startTime, 10L)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                                String.format("expected start time %s, but get %s in show ddl ", estimatedStartTime,
                                    startTime));
                        }
                        if (!checkIfCloseTimeStamp(estimatedEndTime, endTime, 2L)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                                String.format("expected end time %s, but get %s in show ddl ", estimatedEndTime,
                                    endTime));
                        }
                        flag = true;
                    }
                }
            }
            JdbcUtil.executeQuerySuccess(connection1, " set global MAX_SHOW_DDL_RESULT_STMT_LENGTH=128");
            results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(connection1, "show ddl result"));
            for (List<Object> result : results) {
                String truncatedSql = result.get(6).toString();
                if (truncatedSql.length() > 128 && (!truncatedSql.endsWith("...") || truncatedSql.length() >= 132)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        String.format("expected truncate and format sql, but get %s", truncatedSql));
                }
            }
        }
        if (!flag) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("not found ddl result %s in show ddl ", sql));

        }
    }

    public String generateColumnName() {
        seq += 1;
        return String.format("very_long_column_%046d", seq);
    }

    public String generateTableName(int partNum) {
        return String.format("very_long_table_name_%035d%s", partNum, randomTableName("", 4));
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
    public void testCreateTable1024Partitions() throws SQLException, InterruptedException {
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
        int maxWaitTimeout = 180;
        String checkPhyTableFailedHint =
            String.format(
                "/*+TDDL:CMD_EXTRA(EMIT_PHY_TABLE_DDL_DELAY=%d, CREATE_TABLE_SKIP_CDC=true, ACQUIRE_CREATE_TABLE_GROUP_LOCK=false)*/",
                largeDelay);
        sql = checkPhyTableFailedHint + sql;

        String errMsg = "doesn't exist";
        String otherErrMsg = "Execute AlterTableCheckPhyTableDoneFailed timeout.";
        innerTest(mytable, tddlConnection, sql, maxWaitTimeout, otherErrMsg);
    }

    @Test
    public void testCreateTable2048Partitions() throws SQLException, InterruptedException {
        String mytable = generateTableName(2048);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String partitionByColumnName = generateColumnName();
        String createTableStmt = "create table " + createOption + " %s" + generateColumnDef(partitionByColumnName, 40) +
            generatePartitionByDef(partitionByColumnName, 2048);
        String sql = String.format(createTableStmt, mytable);
        int maxWaitTimeout = 360;
        String checkPhyTableFailedHint =
            String.format(
                "/*+TDDL:CMD_EXTRA(EMIT_PHY_TABLE_DDL_DELAY=%d, CREATE_TABLE_SKIP_CDC=true, ACQUIRE_CREATE_TABLE_GROUP_LOCK=false)*/",
                largeDelay);
        sql = checkPhyTableFailedHint + sql;

        String errMsg = "doesn't exist";
        String otherErrMsg = "Execute AlterTableCheckPhyTableDoneFailed timeout.";
        innerTest(mytable, tddlConnection, sql, maxWaitTimeout, otherErrMsg);
    }

    boolean checkIfCloseTimeStamp(String before, String after, long delta) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 解析字符串为LocalDateTime对象
        LocalDateTime beforeTime = LocalDateTime.parse(before, formatter);
        LocalDateTime afterTime = LocalDateTime.parse(after, formatter);
        long s1 = beforeTime.atZone(ZoneId.systemDefault()).toEpochSecond();
        long s2 = afterTime.atZone(ZoneId.systemDefault()).toEpochSecond();
        return Math.abs(s1 - s2) <= delta;

    }
}
