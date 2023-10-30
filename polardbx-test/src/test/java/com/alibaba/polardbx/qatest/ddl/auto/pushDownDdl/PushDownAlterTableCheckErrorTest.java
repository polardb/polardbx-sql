package com.alibaba.polardbx.qatest.ddl.auto.pushDownDdl;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.mdl.MdlDetectionTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@NotThreadSafe
@RunWith(Parameterized.class)
public class PushDownAlterTableCheckErrorTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(PushDownAlterTableCheckErrorTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public PushDownAlterTableCheckErrorTest(boolean crossSchema) {
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
        this.tableName = schemaPrefix + randomTableName("pushdown", 4);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private Future<Boolean> executeSqlAndCommitFailed(ExecutorService threadPool, String tableName,
                                                      Connection connection, String sql, String errMsg) {
        return threadPool.submit(() -> {
            try {
                log.info("execute ddl " + sql);
                JdbcUtil.executeUpdateFailed(connection, sql, errMsg);
            } catch (Exception e) {
                log.info(e.getMessage());
                return false;
            }
            return true;
        });
    }

    private Future<Boolean> executeRandomDeletePhyTable(ExecutorService threadPool, String tableName,
                                                        String createTableStmt,
                                                        Connection connection,
                                                        List<Integer> indexes) {
        List<Pair<Integer, String>> objectInfos =
            indexes.stream().map(o -> getFullObjectName(connection, tableName, tableName, o.intValue())).collect(
                Collectors.toList());
        return threadPool.submit(() -> {
            Boolean checkOk = false;
            try {
                Thread.sleep(smallDelay * 1000L);
                for (Pair<Integer, String> objectInfo : objectInfos) {
                    String groupHint = String.format("/*+TDDL:node(%d)*/", objectInfo.getKey());
                    String dropTableStmt = "drop table `%s`";
                    String dropTableSql = groupHint + String.format(dropTableStmt, objectInfo.getValue());
                    logger.info("execute SQL:" + dropTableSql);
                    JdbcUtil.executeUpdateSuccess(connection, dropTableSql);
                }
                Boolean rollbackPauseState = false;
                String schemaName =
                    JdbcUtil.getAllResult(JdbcUtil.executeQuery("select database()", connection)).get(0).get(0)
                        .toString();
                while (!rollbackPauseState) {
                    Thread.sleep(5000);
                    String queryDdlState = String.format(
                        "select job_id, state from metadb.ddl_engine where schema_name = '%s' and object_name = '%s'",
                        schemaName, tableName);
                    List<List<Object>> results =
                        JdbcUtil.getAllResult(JdbcUtil.executeQuery(queryDdlState, connection));
                    rollbackPauseState =
                        results.isEmpty() || (results.get(0).get(1).toString().equalsIgnoreCase("ROLLBACK_COMPLETED"));
                }

                for (Pair<Integer, String> objectInfo : objectInfos) {
                    String groupHint = String.format("/*+TDDL:node(%d)*/", objectInfo.getKey());
                    String createTableSql = groupHint + String.format(createTableStmt, objectInfo.getValue());

                    logger.info("execute SQL:" + createTableSql);
                    JdbcUtil.executeUpdateSuccess(connection, createTableSql);
                }
                String checkTableSql = String.format("check table %s", tableName);
                logger.info("execute SQL:" + checkTableSql);
                List<List<Object>> checkTableResult =
                    JdbcUtil.getAllResult(JdbcUtil.executeQuery(checkTableSql, connection));
                logger.info("check table result: " + checkTableResult.toString());
                checkOk = checkTableResult.stream().noneMatch(o -> !(o.get(3).toString().equalsIgnoreCase("OK")));
            } catch (Throwable e) {
                return false;
            }
            return checkOk;
        });
    }

    void innerTest(String tableName, Connection connection, String sql, int maxWaitTimeout, String createTableStmt,
                   List<Integer> dropPhyTableIndexes,
                   String expectedErrMsg,
                   String otherErrMsg) throws SQLException {
        final ExecutorService ddlThreadPool = new ThreadPoolExecutor(2, 2, 0L,
            TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
            new NamedThreadFactory(MdlDetectionTest.class.getSimpleName(), false));
        final List<Future<Boolean>> futures = new LinkedList<>();

        // Connection 1: ddl
        Connection connection1 = getPolardbxConnection();
        futures.add(
            executeRandomDeletePhyTable(ddlThreadPool, tableName, createTableStmt, connection1, dropPhyTableIndexes));
        futures.add(executeSqlAndCommitFailed(ddlThreadPool, tableName, tddlConnection, sql, expectedErrMsg));

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

    @Test
    public void testAlterTableCheckPhyTableDoneFailed() throws SQLException {
        String mytable = schemaPrefix + randomTableName("alter_table_check_phy_table_failed", 4);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String createTableStmt = "create table " + createOption + " %s(a int,b char, d int, primary key(d))";
        String sql = String.format(createTableStmt, mytable);
        int maxWaitTimeout = 180;
        String checkPhyTableFailedHint = String.format("/*+TDDL:CMD_EXTRA(EMIT_PHY_TABLE_DDL_DELAY=%d)*/", largeDelay);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            checkPhyTableFailedHint + "ALTER TABLE %s ADD COLUMN c int",
            mytable);
        String errMsg = "doesn't exist";
        String otherErrMsg = "Execute AlterTableCheckPhyTableDoneFailed timeout.";
        List<Integer> dropPhyTableIndexes = Collections.singletonList(1);
        innerTest(mytable, tddlConnection, sql, maxWaitTimeout, createTableStmt, dropPhyTableIndexes, errMsg,
            otherErrMsg);
    }

    @Test
    public void testAlterTableCheckPhyTableDoneFailedMultiplePartitions() throws SQLException {
        String mytable = schemaPrefix + randomTableName("alter_table_check_phy_table_failed", 4);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String createTableStmt =
            "create table " + createOption + " %s(a int,b char, d int, primary key(d), index i_a(a))";
        String partitionBy = " partition by hash(a) partitions 32";
        String sql = String.format(createTableStmt + partitionBy, mytable);
        int maxWaitTimeout = 180;
        String checkPhyTableFailedHint = String.format("/*+TDDL:CMD_EXTRA(EMIT_PHY_TABLE_DDL_DELAY=%d)*/", largeDelay);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            checkPhyTableFailedHint + "ALTER TABLE %s ADD COLUMN c int",
            mytable);
        String errMsg = "doesn't exist";
        String otherErrMsg = "Execute AlterTableCheckPhyTableDoneFailed timeout.";
        List<Integer> dropPhyTableIndexes = Arrays.asList(5, 7, 8, 12);
        innerTest(mytable, tddlConnection, sql, maxWaitTimeout, createTableStmt, dropPhyTableIndexes, errMsg,
            otherErrMsg);
    }

    public Pair<Integer, String> getFullObjectName(Connection connection, String tableName, String objectName,
                                                   int index) {
        String fetchNameSql = String.format("show full create table %s", objectName);
        ResultSet resultSet1 = JdbcUtil.executeQuery(fetchNameSql, tddlConnection);
        String fullTableName = JdbcUtil.getAllResult(resultSet1).get(0).get(0).toString();
        String fetchTopology = String.format("show topology %s", fullTableName);
        ResultSet resultSet2 = JdbcUtil.executeQuery(fetchTopology, tddlConnection);
        List<Object> result =
            JdbcUtil.getAllResult(resultSet2).stream().filter(o -> o.get(2).toString().endsWith(String.valueOf(index)))
                .collect(Collectors.toList()).get(0);
//            +----+-----------------+---------------+----------------+-------------+---------------------------------+
//            | ID | GROUP_NAME      | TABLE_NAME    | PARTITION_NAME| PARENT_PARTITION_NAME | PHY_DB_NAME | DN_ID                           |
//            +----+-----------------+---------------+----------------+-------------+---------------------------------+
//            | 0  | D1_P00002_GROUP | t1_cLVA_00001 | p2           | p2sp1  | d1_p00002   | pxc-xdb-s-pxchzrwy270yxoiww3934 |
        String physicalTableName = result.get(2).toString();
        String physicalDbName = result.get(5).toString();
        Integer groupIndex = Integer.valueOf(physicalDbName.substring(physicalDbName.length() - 5));
        return Pair.of(groupIndex, physicalTableName);
    }

}
