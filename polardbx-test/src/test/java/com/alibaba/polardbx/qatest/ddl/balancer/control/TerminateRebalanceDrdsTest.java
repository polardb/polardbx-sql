package com.alibaba.polardbx.qatest.ddl.balancer.control;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TerminateRebalanceDrdsTest extends DDLBaseNewDBTestCase {
    private static final String SLOW_HINT = "GSI_DEBUG=\"slow\"";
    private static final String ASYNC_DDL_HINT = "ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true";
    private static final String ENABLE_CHANGESET_HINT = "CN_ENABLE_CHANGESET=%s";
    // 本地测试环境需要加该hint 或者 set global
    private static final String LOCAL_HINT =
        "SHARE_STORAGE_MODE=true,PHYSICAL_BACKFILL_ENABLE=false,SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true";
    private static final String DROP_DB_HINT = "ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE=true";

    private static final int TABLE_COUNT = 4;
    static private final String DATABASE_NAME = "TerminateRebalanceDrdsTest";
    private static final String TABLE_PREFIX = "tb";
    private static final String MOVE_GROUP_1 = "TERMINATEREBALANCEDRDSTEST_000001_GROUP";
    private static final String MOVE_GROUP_0 = "TERMINATEREBALANCEDRDSTEST_000000_GROUP";
    private static final String SHOW_DS = "show ds where db='%s'";

    private static final String CREATE_TABLE_SQL =
        "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP) ";

    private static final String DROP_TABLE_SQL = "drop table if exists `%s` ";

    protected boolean enableChangeSet;

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    @Parameterized.Parameters(name = "{index}:enableChangeSet={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{true}});
    }

    public TerminateRebalanceDrdsTest(boolean enableChangeSet) {
        this.enableChangeSet = enableChangeSet;
    }

    @Before
    public void before() {
        doReCreateDatabase();
    }

    @After
    public void after() {
        doClearDatabase();
    }

    @Test
    public void testTerminateRebalance() throws SQLException, InterruptedException {
        // create table
        for (int i = 0; i < TABLE_COUNT; ++i) {
            String tbName = TABLE_PREFIX + i;
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(DROP_TABLE_SQL, tbName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_SQL, tbName));
        }

        // move partitions
        String moveDbCommand = prepareDrdsDbCommands(tddlConnection);
        String hint = buildCmdExtra(SLOW_HINT, ASYNC_DDL_HINT, String.format(ENABLE_CHANGESET_HINT, enableChangeSet)
            // , LOCAL_HINT
        );
        JdbcUtil.executeUpdateSuccess(tddlConnection, "move database " + hint + moveDbCommand);

        // check
        Long jobId = getDDLJobId(tddlConnection);
        Thread.sleep(2000);
        if (!checkRunningDDL(tddlConnection)) {
            // ignore
            return;
        }

        // rollback
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("TERMINATE REBALANCE %s", jobId));

        // check
        Assert.assertTrue(waitDDLJobFinish(tddlConnection));
    }

    @Test
    public void testTerminateRebalanceDrainNodeSchedule() throws SQLException, InterruptedException {
        // create table
        for (int i = 0; i < TABLE_COUNT; ++i) {
            String tbName = TABLE_PREFIX + i;
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(DROP_TABLE_SQL, tbName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_SQL, tbName));
        }

        String command = prepareDrainNodeCommand(tddlConnection);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "schedule " + command);

        // check
        Thread.sleep(10500);
        Long jobId = getDDLJobId(tddlConnection);
        // Assert.assertTrue(checkRunningDDL(tddlConnection));
        if (!checkRunningDDL(tddlConnection)) {
            return;
        }
        Assert.assertTrue(jobId != -1L);

        // rollback
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("TERMINATE REBALANCE %s", jobId));

        // check
        Assert.assertTrue(waitDDLJobFinish(tddlConnection));

        Assert.assertTrue(getDDLPlanStats(tddlConnection, jobId).equalsIgnoreCase("TERMINATED"));
    }

    @Test
    public void testTerminateRebalanceSchedule() throws SQLException, InterruptedException {
        // create table
        for (int i = 0; i < TABLE_COUNT; ++i) {
            String tbName = TABLE_PREFIX + i;
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(DROP_TABLE_SQL, tbName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_SQL, tbName));
        }

        String command = prepareDrainNodeCommand(tddlConnection);
        JdbcUtil.executeUpdateSuccess(tddlConnection, command + " async=false");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "schedule rebalance database");

        // check
        Thread.sleep(10500);
        Long jobId = getDDLJobId(tddlConnection);
        // Assert.assertTrue(checkRunningDDL(tddlConnection));
        if (!checkRunningDDL(tddlConnection)) {
            return;
        }
        Assert.assertTrue(jobId != -1L);

        // rollback
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("TERMINATE REBALANCE %s", jobId));

        // check
        Assert.assertTrue(waitDDLJobFinish(tddlConnection));

        Assert.assertTrue(getDDLPlanStats(tddlConnection, jobId).equalsIgnoreCase("SUCCESS"));
    }

    private static boolean checkRunningDDL(Connection connection) throws SQLException {
        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);

        sql = "show ddl";
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (rs.next()) {
            String ddlState = rs.getString("STATE");
            rs.close();
            return ddlState.equalsIgnoreCase("RUNNING");
        }
        return false;
    }

    private static boolean waitDDLJobFinish(Connection connection) throws SQLException, InterruptedException {
        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);

        int count = 0;
        while (count < 15) {
            sql = "show ddl";
            ResultSet rs = JdbcUtil.executeQuery(sql, connection);
            if (!rs.next()) {
                rs.close();
                return true;
            }
            Thread.sleep(2000);
            count++;
        }
        return false;
    }

    private static Long getDDLJobId(Connection connection) throws SQLException {
        long jobId = -1L;
        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);

        sql = "show ddl";
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (rs.next()) {
            jobId = rs.getLong("JOB_ID");
        }
        rs.close();
        return jobId;
    }

    private static String prepareDrainNodeCommand(Connection connection) throws SQLException {
        String dnId = null;
        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);

        sql = String.format(SHOW_DS, DATABASE_NAME);
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        while (rs.next()) {
            if (rs.getString("GROUP").equalsIgnoreCase(MOVE_GROUP_1)) {
                dnId = rs.getString("STORAGE_INST_ID");
                break;
            }
        }
        rs.close();

        Assert.assertTrue(dnId != null);

        return String.format("rebalance database drain_node = '%s'", dnId);
    }

    private static String getDDLPlanStats(Connection connection, Long jobId) throws SQLException {
        String state = null;
        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);

        sql = String.format("select state from metadb.ddl_plan where job_id = %s", jobId);
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (rs.next()) {
            state = rs.getString("state");
        }
        rs.close();
        return state;
    }

    private static String prepareDrdsDbCommands(Connection connection) throws SQLException {
        String targetInstId = null;

        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);

        sql = String.format(SHOW_DS, DATABASE_NAME);
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        while (rs.next()) {
            if (rs.getString("GROUP").equalsIgnoreCase(MOVE_GROUP_0)) {
                targetInstId = rs.getString("STORAGE_INST_ID");
                break;
            }
        }
        rs.close();

        Assert.assertTrue(targetInstId != null);

        return MOVE_GROUP_1 + " to " + String.format("'%s'", targetInstId);
    }

    void doReCreateDatabase() {
        doClearDatabase();
        String createDbHint = "/*+TDDL({\"extra\":{\"SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT\":\"1\"}})*/";
        String tddlSql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = createDbHint + "create database " + DATABASE_NAME + " partition_mode = 'drds'";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
    }

    void doClearDatabase() {
        JdbcUtil.executeUpdate(getTddlConnection1(), "use information_schema");
        String tddlSql = buildCmdExtra(DROP_DB_HINT) + "drop database if exists " + DATABASE_NAME;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }
}
