package com.alibaba.polardbx.qatest.twoPhaseDdl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.prepareData;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.alterTableViaTwoPhaseDdl;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.checkIfExecuteTwoPhaseDdl;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.checkPhyDdlStatus;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.checkTableStatus;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.continueDdl;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.getDdlJobIdFromPattern;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.pauseDdl;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.waitTillDdlDone;

@NotThreadSafe
@RunWith(Parameterized.class)
public class TwoPhaseDdlSecondaryPartitionDdlTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(TwoPhaseDdlCheckApplicabilityTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public TwoPhaseDdlSecondaryPartitionDdlTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    public int smallDelay = 1;

    public int largeDelay = 5;

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}});
    }

    @Before
    public void init() {
        this.tableName = schemaPrefix + randomTableName("two_phase", 4);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    //two_phase_ddl_test1.online_ddl_pause_subpart, 16 * 50W
    @Test
    public void testConcurrentAlterTableAddDdlPauseBeforePrepare() throws SQLException, InterruptedException {
        String schemaName = "two_phase_ddl_test1";
        List<String> tableNames = new ArrayList<>();
        List<Connection> connections = new ArrayList<>();
        int concurrent = 1;
        for (int i = 0; i < concurrent; i++) {
            String tableName = schemaPrefix + "online_ddl_pause_subpart" + String.valueOf(i);
            tableNames.add(tableName);
            prepareData(tddlConnection, schemaName, tableName, 500_000,
                DataManipulateUtil.TABLE_TYPE.SECONDARY_PARTITION_TABLE);
        }
        for (int i = 0; i < concurrent; i++) {
            Connection tddlConnection = getPolardbxConnection(schemaName);
            connections.add(tddlConnection);
        }
        //
        ExecutorService executorService = Executors.newFixedThreadPool(concurrent);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < concurrent; i++) {
            String columName = randomTableName("column", 2);
            String ddl =
                String.format("alter table %s add column %s int, ALGORITHM=INPLACE", tableNames.get(i), columName);
            int finalI = i;
            futures.add(
                executorService.submit(() -> {
                    try {
                        runTestCaseOnOneTable(connections.get(finalI), schemaName, tableNames.get(
                            finalI), ddl);
                    } catch (SQLException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
            );
        }
        try {
            for (int i = 0; i < concurrent; i++) {
                Future<?> future = futures.get(i);
                future.get();
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, cause.getMessage());
        }
    }

    //two_phase_ddl_test1.modify_column_pause_subpart, 16 * 50W
    @Test
    public void testConcurrentAlterTableModifyColumnDdlPauseBeforePrepare() throws SQLException, InterruptedException {
        String schemaName = "two_phase_ddl_test1";
        List<String> tableNames = new ArrayList<>();
        List<Connection> connections = new ArrayList<>();
        int concurrent = 1;
        for (int i = 0; i < concurrent; i++) {
            String tableName = schemaPrefix + "modify_column_pause_subpart" + String.valueOf(i);
            tableNames.add(tableName);
            prepareData(tddlConnection, schemaName, tableName, 500_000,
                DataManipulateUtil.TABLE_TYPE.SECONDARY_PARTITION_TABLE);
        }
        for (int i = 0; i < concurrent; i++) {
            Connection tddlConnection = getPolardbxConnection(schemaName);
            connections.add(tddlConnection);
        }
        //
        ExecutorService executorService = Executors.newFixedThreadPool(concurrent);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < concurrent; i++) {
            String table = tableNames.get(i);
            String recoverDdl = String.format("alter table %s modify column c varchar(32)", table);
            alterTableViaTwoPhaseDdl(tddlConnection, schemaName, table, recoverDdl);
            String ddl = String.format("alter table %s modify column c varchar(16)", table);
            int finalI = i;
            futures.add(
                executorService.submit(() -> {
                    try {
                        runTestCaseOnOneTable(connections.get(finalI), schemaName, tableNames.get(
                            finalI), ddl);
                    } catch (SQLException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                })
            );
        }
        try {
            for (int i = 0; i < concurrent; i++) {
                Future<?> future = futures.get(i);
                future.get();
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, cause.getMessage());
        }
    }

    public void runTestCaseOnOneTable(Connection tddlConnection, String schemaName, String table, String ddl)
        throws SQLException, InterruptedException {
        String enableTwoPhaseDdlHint =
            String.format("/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true,PURE_ASYNC_DDL_MODE=true)*/");
        String msg = String.format("table: %s, ddl: %s", table, ddl);
        alterTableViaTwoPhaseDdl(tddlConnection, schemaName, table, enableTwoPhaseDdlHint + ddl);
        Long jobId = getDdlJobIdFromPattern(tddlConnection, ddl);
        int sleepTime = 1;
        Thread.sleep(sleepTime * 1000);
        int i = 0;
        while (i < 2) {
            pauseDdl(tddlConnection, jobId);
            if (!checkPhyDdlStatus(schemaName, tddlConnection, jobId, sleepTime * 2, table)) {
                continueDdl(tddlConnection, jobId);
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("pause ddl failed! there are still physical ddl emitting for %s", msg));
            }
            Thread thread = new Thread(() -> {
                try (Connection connection = getPolardbxConnection(schemaName)) {
                    continueDdl(connection, jobId);
                } catch (SQLException ignored) {
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
            Thread.sleep(sleepTime * 1000);
            i++;
        }
        if (!waitTillDdlDone(tddlConnection, jobId, table)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("wait ddl done timeout for %s", msg));
        }
        if (!checkIfExecuteTwoPhaseDdl(tddlConnection, jobId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("this job is not two phase ddl for %s", msg));
        }
        if (!checkTableStatus(tddlConnection, jobId, table)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("check table failed for %s, after continue ddl finished", msg));
        }
    }
}
