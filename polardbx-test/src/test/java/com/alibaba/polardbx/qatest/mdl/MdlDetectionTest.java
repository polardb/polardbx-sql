package com.alibaba.polardbx.qatest.mdl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.NotThreadSafe.DeadlockTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@NotThreadSafe
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MdlDetectionTest extends DDLBaseNewDBTestCase {

    static Long maxWaitTimeout = 30L;

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private static final Log logger = LogFactory.getLog(DeadlockTest.class);

    @Test(timeout = 600000)
    public void testAllBefore() throws Exception {
        final List<Connection> connections = new ArrayList<>(3);
        String updatePerformanceSchemaSetUpConsumersSql =
            "UPDATE performance_schema.setup_consumers SET ENABLED ='YES' WHERE NAME='global_instrumentation';";
        String updatePerformanceSchemaSetUpInstrumentsSql =
            "UPDATE performance_schema.setup_instruments SET ENABLED ='YES' WHERE NAME='wait/lock/metadata/sql/mdl';";

        String showPerformanceSchemaSetUpConsumersSql =
            "SELECT ENABLED from performance_schema.setup_consumers WHERE NAME='global_instrumentation';";
        String showPerformanceSchemaSetUpInstrumentsSql =
            "SELECT ENABLED from performance_schema.setup_instruments where NAME='wait/lock/metadata/sql/mdl';";
        String formatHint = "/*+TDDL:node(%d)*/";

        for (int i = 0; i < 3; i++) {
            connections.add(getPolardbxConnection());
        }
        int dnNum = 2;
        try {
            dnNum = JdbcUtil.getAllResult(
                JdbcUtil.executeQuerySuccess(connections.get(0), "show storage where INST_KIND=\"MASTER\";")).size();
            for (int i = 0; i < dnNum; i++) {
                String hint = String.format(formatHint, i);
                JdbcUtil.executeUpdateSuccess(connections.get(0), hint + updatePerformanceSchemaSetUpConsumersSql);
                JdbcUtil.executeUpdateSuccess(connections.get(0), hint + updatePerformanceSchemaSetUpInstrumentsSql);
            }
            for (int i = 0; i < dnNum; i++) {
                String hint = String.format(formatHint, i);
                String result = JdbcUtil.getAllResult(
                        JdbcUtil.executeQuerySuccess(connections.get(0), hint + showPerformanceSchemaSetUpConsumersSql))
                    .get(0).get(0).toString();
                Assert.assertTrue("set performance schema failed!", result.equalsIgnoreCase("YES"));
                result = JdbcUtil.getAllResult(
                        JdbcUtil.executeQuerySuccess(connections.get(0), hint + showPerformanceSchemaSetUpInstrumentsSql))
                    .get(0).get(0).toString();
                Assert.assertTrue("set performance schema failed!", result.equalsIgnoreCase("YES"));
            }
        } catch (Exception exception) {
            throw new Exception("set performance schema failed for " + exception.toString());
        }
    }

    @Test(timeout = 600000)
    public void testMdlDetectionVariablesSetting() throws SQLException {
        int sampleTimeout = 3600000;
        final String tableName = "mdl_detection_global_variables_setting";
        final String createTableStmt =
            String.format("create table %s(a int, b int) partition by hash(a) partitions 16;", tableName);
        String ddlStmt = String.format("alter table %s add column x1 int", tableName);

        final List<Connection> connections = new ArrayList<>(2);
        for (int i = 0; i < 3; i++) {
            connections.add(getPolardbxConnection());
        }
        try {
            createTable(tableName, false, createTableStmt);
            // Connection 0: select for update
            JdbcUtil.executeQuerySuccess(connections.get(0), "begin");
            String sql = "select * from " + tableName + "  for update";
            JdbcUtil.executeQuerySuccess(connections.get(0), sql);

            final ExecutorService threadPool = new ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
                new NamedThreadFactory(MdlDetectionTest.class.getSimpleName(), false));

            // shutdown
            String setGlobalTimeoutSql = "set global " + ConnectionParams.PHYSICAL_DDL_MDL_WAITING_TIMEOUT + " = %d";
            JdbcUtil.executeQuerySuccess(connections.get(0), String.format(setGlobalTimeoutSql, 0));
            int defaultTimeout = 60;
            // Connection 1: ddl
            Future<Boolean> future = executeSqlAndCommit(threadPool, tableName, connections.get(1), ddlStmt);

            try {
                logger.info("mdl detection shutdown, and wait for the first time");
                Thread.sleep(defaultTimeout * 1000L);
                if (future.isDone()) {
                    Assert.fail("Mdl Detection: switch failed!");
                }
                // launch
                logger.info("mdl detection launched, and wait for the second time");
                JdbcUtil.executeQuerySuccess(connections.get(2), String.format(setGlobalTimeoutSql, 5));
                defaultTimeout = 30;
                if (!future.get(defaultTimeout, TimeUnit.SECONDS)) {
                    Assert.fail("Mdl Detection: switch failed!");
                }

                // long time
                logger.info("mdl detection timeout reset to 100 seconds, and wait for the third time");
                JdbcUtil.executeQuerySuccess(connections.get(2), String.format(setGlobalTimeoutSql, 100));
                JdbcUtil.executeQuerySuccess(connections.get(2), "begin");
                sql = "select * from " + tableName + "  for update";
                JdbcUtil.executeQuerySuccess(connections.get(2), sql);
                ddlStmt = String.format("alter table %s add column x2 int", tableName);
                future = executeSqlAndCommit(threadPool, tableName, connections.get(1), ddlStmt);

                defaultTimeout = 50;
                Thread.sleep(defaultTimeout * 1000L);
                if (future.isDone()) {
                    Assert.fail("Mdl Detection: switch failed!");
                }

                defaultTimeout = 30;
                logger.info("mdl detection timeout reset to 15 seconds, and wait for the forth time");
                JdbcUtil.executeQuerySuccess(connections.get(2), String.format(setGlobalTimeoutSql, 15));
                if (!future.get(defaultTimeout, TimeUnit.SECONDS)) {
                    Assert.fail("Mdl Detection: switch failed!");
                }
            } catch (TimeoutException e) {
                e.printStackTrace();
                Assert.fail("Mdl Detection: Wait for too long, more than maxWaitTimeout seconds in !" + getClass());
            } catch (Exception e) {
                Assert.fail("Mdl Detection: failed for unexpected cause!");
            }
        } finally {
            clear(connections, tableName);
        }
    }

    @Test(timeout = 60000)
    public void testExlusiveMdlWaitingForAddColumn() throws SQLException {
        final String tableName = "mdl_waiting_test_add_column";
        final String createTableStmt =
            String.format("create table %s(a int, b int) partition by hash(a) partitions 16;", tableName);
        final String ddlStmt = String.format("alter table %s add column x1 int", tableName);
        testFramework(tableName, false, ddlStmt, createTableStmt);
    }

    @Test(timeout = 60000)
    public void testExlusiveMdlWaitingForModifyColumn() throws SQLException {
        final String tableName = "mdl_waiting_test_modify_column";
        final String createTableStmt =
            String.format("create table %s(a int, b int) partition by hash(a) partitions 16;", tableName);
        final String ddlStmt = String.format("alter table %s modify column b bigint", tableName);
        testFramework(tableName, false, ddlStmt, createTableStmt);
    }

    @Test(timeout = 60000)
    public void testExlusiveMdlWaitingForDropColumn() throws SQLException {
        final String tableName = "mdl_waiting_test_drop_column";
        final String createTableStmt =
            String.format("create table %s(a int, b int) partition by hash(a) partitions 16;", tableName);
        final String ddlStmt = String.format("alter table %s drop column b", tableName);
        testFramework(tableName, false, ddlStmt, createTableStmt);
    }

    @Test(timeout = 60000)
    public void testExlusiveMdlWaitingForDropTable() throws SQLException {
        final String tableName = "mdl_waiting_test_drop_table";
        final String createTableStmt =
            String.format("create table %s(a int, b int) partition by hash(a) partitions 16;", tableName);
        final String ddlStmt = String.format("drop table %s;", tableName);
        testFramework(tableName, false, ddlStmt, createTableStmt);
    }

    @Test(timeout = 60000)
    public void testExlusiveMdlWaitingForRepartitionTable() throws SQLException {
        final String tableName = "mdl_waiting_test_repartition_table";
        final String createTableStmt =
            String.format("create table %s(a int, b int) partition by hash(a) partitions 16;", tableName);
        final String ddlStmt = String.format("alter table %s partition by hash(a, b) partitions 8;", tableName);
        testFramework(tableName, false, ddlStmt, createTableStmt);
    }

    @Test(timeout = 60000)
    public void testExlusiveMdlWaitingForAddLocalIndex() throws SQLException {
        final String tableName = "mdl_waiting_test_add_local_index";
        final String createTableStmt =
            String.format("create table %s(a int, b int) partition by hash(a) partitions 16;", tableName);
        final String ddlStmt = String.format("alter table %s add index i_a_b(a, b)", tableName);
        testFramework(tableName, false, ddlStmt, createTableStmt);
    }

    @Test(timeout = 60000)
    public void testExlusiveMdlWaitingForOptimizeTable() throws SQLException {
        final String tableName = "mdl_waiting_test_optimize_table";
        final String createTableStmt =
            String.format("create table %s(a int, b int) partition by hash(a) partitions 16;", tableName);
        final String ddlStmt = String.format("optimize table %s", tableName);
        testFramework(tableName, false, ddlStmt, createTableStmt);
    }

    @Test(timeout = 60000)
    public void testExlusiveMdlWaitingForAlterLocalPartition() throws SQLException {
        final String tableName = "mdl_waiting_test_alter_local_partition";
        String createTableStmt = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 6\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", tableName);
        // change the definition of modify partition
        String ddlStmt = String.format("alter table %s \n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", tableName);
        testFramework(tableName, false, ddlStmt, createTableStmt);
    }

    @Test(timeout = 60000)
    public void testExlusiveMdlWaitingForExpireLocalPartition() throws SQLException {
        final String tableName = "mdl_waiting_test_expire_local_partition";
        String createTableStmt = String.format("CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '2023-04-01'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 1\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";", tableName);
        // expire local partition
        String ddlStmt = String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION p20230501\n"
            + ";", tableName);
        testFramework(tableName, false, ddlStmt, createTableStmt);
    }

    private void testFramework(String tableName, boolean single, String ddlStmt, String createTableStmt)
        throws SQLException {
        final List<Connection> connections = new ArrayList<>(2);
        for (int i = 0; i < 2; i++) {
            connections.add(getPolardbxConnection());
        }
        try {
            createTable(tableName, single, createTableStmt);
            innerTest(tableName, connections, ddlStmt);
        } finally {
            clear(connections, tableName);
        }
    }

    private void innerTest(String tableName, List<Connection> connections, String ddl) {

//        String sql = "insert into " + tableName + " values (0), (1)";
//        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Connection 0: select for update
        JdbcUtil.executeQuerySuccess(connections.get(0), "begin");
        String sql = "select * from " + tableName + "  for update";
        JdbcUtil.executeQuerySuccess(connections.get(0), sql);

        final ExecutorService threadPool = new ThreadPoolExecutor(1, 1, 0L,
            TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
            new NamedThreadFactory(MdlDetectionTest.class.getSimpleName(), false));
        final List<Future<Boolean>> futures = new LinkedList<>();

        // Connection 1: ddl
        futures.add(executeSqlAndCommit(threadPool, tableName, connections.get(1), ddl));

        for (Future<Boolean> future : futures) {
            try {
                if (!future.get(maxWaitTimeout, TimeUnit.SECONDS)) {
                    Assert.fail("Mdl Detection: Logical ddl failed!");
                }
            } catch (TimeoutException e) {
                e.printStackTrace();
                Assert.fail("Mdl Detection: Wait for too long, more than maxWaitTimeout seconds in !" + getClass());
            } catch (Exception e) {
                Assert.fail("Mdl Detection: failed for unexpected cause!");
            }
        }
    }

    private void clear(Collection<Connection> connections, String tableName) {
        for (Connection connection : connections) {
            if (null != connection) {
                try {
                    JdbcUtil.executeQuerySuccess(connection, "commit");
                } catch (Throwable e) {
                    // ignore
                    e.printStackTrace();
                }
                try {
                    connection.close();
                } catch (Throwable e) {
                    // ignore
                    e.printStackTrace();
                }
            }
        }
    }

    private Future<Boolean> executeSqlAndCommit(ExecutorService threadPool, String tableName,
                                                Connection connection, String sql) {
        return threadPool.submit(() -> {
            try {
                JdbcUtil.executeUpdate(connection, sql);
            } catch (Throwable e) {
                return false;
            }
            return true;
        });
    }

    private void createTable(String tableName, boolean single, String createTableStmt) {
        String sql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Create a partition table
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableStmt);
    }
}

