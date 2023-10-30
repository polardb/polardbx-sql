package com.alibaba.polardbx.qatest.ddl.balancer.control;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class SkipRebalanceSubjobDrdsTest extends DDLBaseNewDBTestCase {
    private static final String SLOW_HINT = "GSI_DEBUG=\"slow\"";
    private static final String ENABLE_CHANGESET_HINT = "CN_ENABLE_CHANGESET=%s";
    // 本地测试环境需要加该hint 或者 set global
    private static final String LOCAL_HINT =
        "SHARE_STORAGE_MODE=true,SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true";
    private static final String ENABLE_OPERATE_SUBJOB_HINT = "ENABLE_OPERATE_SUBJOB=true";
    private static final String ASYNC_PAUSE_HINT = "ASYNC_PAUSE=false";
    private static final String CHECK_RESPONSE_IN_MEM_HINT = "CHECK_RESPONSE_IN_MEM=false";

    private static final int TABLE_COUNT = 4;
    static private final String DATABASE_NAME = "SkipRebalanceSubjobDrdsTest";
    private static final String TABLE_PREFIX = "tb";
    private static final String MOVE_GROUP_1 = "SKIPREBALANCESUBJOBDRDSTEST_000001_GROUP";
    private static final String SHOW_DS = "show ds where db='%s'";

    private static final String CREATE_TABLE_SQL =
        "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP) ";

    private static final String INSERT_SQL = "insert into `%s`(b) values(1)";

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
        return Arrays.asList(new Object[][] {{true}});
    }

    public SkipRebalanceSubjobDrdsTest(boolean enableChangeSet) {
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
    public void testSkipRebalanceSubjobDrainNodeSchedule() throws SQLException, InterruptedException {
        // create table
        for (int i = 0; i < TABLE_COUNT; ++i) {
            String tbName = TABLE_PREFIX + i;
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(DROP_TABLE_SQL, tbName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_SQL, tbName));
        }

        String command = prepareDrainNodeCommand(tddlConnection);
        String hint = buildCmdExtra(SLOW_HINT, String.format(ENABLE_CHANGESET_HINT, enableChangeSet));
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + command);

        // check
        Long jobId = getDDLJobId(tddlConnection);
        Thread.sleep(3000);
        // Assert.assertTrue(checkRunningDDL(tddlConnection));
        if (!checkRunningDDL(tddlConnection)) {
            return;
        }
        Long subJobId = getRunningSubJobId(tddlConnection);

        if (subJobId != -1L) {
            String subJobHint = buildCmdExtra(ENABLE_OPERATE_SUBJOB_HINT, ASYNC_PAUSE_HINT, CHECK_RESPONSE_IN_MEM_HINT);
            // skip
            JdbcUtil.executeUpdateSuccess(tddlConnection, subJobHint + "SKIP REBALANCE SUBJOB " + subJobId);

            // check
            Thread.sleep(3000);
            Assert.assertTrue(checkDDLStats(tddlConnection, jobId, "PAUSED"));
            Assert.assertTrue(checkDDLStats(tddlConnection, subJobId, "PAUSED"));

            JdbcUtil.executeUpdateSuccess(tddlConnection, subJobHint + "CONTINUE REBALANCE " + subJobId);
            JdbcUtil.executeUpdateSuccess(tddlConnection, subJobHint + "CONTINUE REBALANCE " + jobId);
        }

        waitDDLJobFinish(tddlConnection);
    }

    @Test
    @Ignore
    public void testSkipRebalanceSubjobSchedule() throws SQLException, InterruptedException {
        // create table
        for (int i = 0; i < TABLE_COUNT; ++i) {
            String tbName = TABLE_PREFIX + i;
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(DROP_TABLE_SQL, tbName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_SQL, tbName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(INSERT_SQL, tbName));
        }

        String command = prepareDrainNodeCommand(tddlConnection);
        JdbcUtil.executeUpdateSuccess(tddlConnection, command + " async=false");

        String hint = buildCmdExtra(SLOW_HINT, String.format(ENABLE_CHANGESET_HINT, enableChangeSet));
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + "rebalance database");

        // check
        Long jobId = getDDLJobId(tddlConnection);
        Thread.sleep(3000);
        // Assert.assertTrue(checkRunningDDL(tddlConnection));
        if (!checkRunningDDL(tddlConnection)) {
            return;
        }
        Long subJobId = getRunningSubJobId(tddlConnection);

        // skip
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format("SKIP REBALANCE SUBJOB %s", subJobId),
            "The DDL job has been paused or cancelled");

        // check
        Thread.sleep(3000);
        checkDDLStats(tddlConnection, jobId, "COMPLETED");
        checkDDLStats(tddlConnection, subJobId, "PAUSED");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "CONTINUE REBALANCE " + subJobId);

        waitDDLJobFinish(tddlConnection);
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

    private static boolean checkDDLStats(Connection connection, Long jobId, String stats) throws SQLException {
        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);

        sql = "show full ddl";
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        while (rs.next()) {
            Long curJobId = rs.getLong("JOB_ID");
            String ddlState = rs.getString("STATE");
            if (curJobId.equals(jobId)) {
                rs.close();
                return ddlState.equalsIgnoreCase(stats);
            }
        }
        rs.close();
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

    private static Long getRunningSubJobId(Connection connection) throws SQLException {
        long jobId = -1L;
        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);

        sql = "show full ddl";
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        while (rs.next()) {
            String msg = rs.getString("RESPONSE_NODE");
            String state = rs.getString("STATE");
            if (msg.contains("subjob") && state.equalsIgnoreCase("RUNNING")) {
                jobId = rs.getLong("JOB_ID");
            }
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
        String tddlSql = "drop database if exists " + DATABASE_NAME;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }
}