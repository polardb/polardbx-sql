package com.alibaba.polardbx.qatest.ddl.auto.pushDownDdl;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.mdl.MdlDetectionTest;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@NotThreadSafe
@RunWith(Parameterized.class)
public class PushDownAlterTableDdlConcurrentTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(PushDownAlterTableDdlConcurrentTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public PushDownAlterTableDdlConcurrentTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    public int smallDelay = 1;

    public int largeDelay = 15;

    public int timeout = 200;

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

    @Test
    public void testAlterTablePauseDdlForDdlConcurrent() throws SQLException, InterruptedException {
        final ExecutorService ddlThreadPool = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
            new NamedThreadFactory(MdlDetectionTest.class.getSimpleName(), false));
        String mytable = schemaPrefix + randomTableName("alter_table_pause_ddl_for_concurrent", 4);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String createTableStmt = "create table " + createOption
            + " %s(a int,b char, d int, primary key(d)) partition by hash(a) partitions 128";
        String sql = String.format(createTableStmt, mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        logger.info(" create table success!");

        sql = String.format(
            "/*+TDDL:CMD_EXTRA(EMIT_PHY_TABLE_DDL_DELAY=%d,ENABLE_DRDS_MULTI_PHASE_DDL=false,MERGE_CONCURRENT=true,MERGE_DDL_CONCURRENT=true,PREFETCH_SHARDS=4)*/ALTER TABLE %s ADD COLUMN c int async=true",
            smallDelay,
            mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        logger.info(" sleep for " + largeDelay + " s");
        Thread.sleep(largeDelay * 1000);
        Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(tddlConnection, sql);
        logger.info(" now pause ddl " + jobId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "pause ddl " + jobId);
        Boolean paused = DdlStateCheckUtil.checkIfPauseSuccessful(tddlConnection, jobId);
        if (!paused) {
            throw new RuntimeException("failed to pause ddl!");
        }
        logger.info(" sleep for " + largeDelay + " s");
        Thread.sleep(largeDelay * 1000);
        Future<Boolean> future = ddlThreadPool.submit(() -> {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "continue ddl " + jobId);
            return true;
        });
        for (int i = 0; i < timeout; i++) {
            if (future.isDone()) {
                break;
            }
            Thread.sleep(1000);
        }
        Boolean complete = DdlStateCheckUtil.checkIfCompleteSuccessful(tddlConnection, jobId);
        if (!complete) {
            throw new RuntimeException("failed to execute test case!");
        }
    }

}
