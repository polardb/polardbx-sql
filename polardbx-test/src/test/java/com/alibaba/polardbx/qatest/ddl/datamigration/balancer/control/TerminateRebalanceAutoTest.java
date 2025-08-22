package com.alibaba.polardbx.qatest.ddl.datamigration.balancer.control;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.prepareData;

public class TerminateRebalanceAutoTest extends DDLBaseNewDBTestCase {
    private static final String SLOW_HINT = "GSI_DEBUG=\"slow\"";
    private static final String ASYNC_DDL_HINT = "ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true";
    private static final String ENABLE_CHANGESET_HINT = "CN_ENABLE_CHANGESET=%s";
    private static final String DROP_DB_HINT = "ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE=true";

    private static final int TABLE_COUNT = 4;
    static private final String DATABASE_NAME = "TerminateRebalanceAutoTest";
    private static final String TABLE_PREFIX = "tb";
    private static final String TABLE_NAME = "tb1";
    private static final String MOVE_PARTITION_GROUPS = "p1";
    private static final String SHOW_DS = "show ds where db='%s'";
    private static final String MOVE_PARTITION_COMMAND = "alter tablegroup by table %s move partitions %s to '%s'";

    private static final String SELECT_FROM_TABLE_DETAIL =
        "select storage_inst_id,table_group_name from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'";

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

    public TerminateRebalanceAutoTest(boolean enableChangeSet) {
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
        String movePartitionsCommand = prepareAutoDbCommands(tddlConnection);
        if (movePartitionsCommand == null) {
            return;
        }
        String hint = buildCmdExtra(SLOW_HINT, ASYNC_DDL_HINT, String.format(ENABLE_CHANGESET_HINT, enableChangeSet));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(hint + movePartitionsCommand, TABLE_NAME));

        // check
        Long jobId = getDDLJobId(tddlConnection);
        Thread.sleep(2000);
        Assert.assertTrue(checkRunningDDL(tddlConnection));

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

    @Test
    public void testResumeRebalanceSchedule() throws SQLException, InterruptedException {
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

        // RESUME REBALANCE
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("RESUME REBALANCE %s", jobId));

        // check
        Assert.assertTrue(waitDDLJobFinish(tddlConnection));

        Assert.assertTrue(getDDLPlanStats(tddlConnection, jobId).equalsIgnoreCase("EXECUTING"));

        // check
        Thread.sleep(10500);
        jobId = getDDLJobId(tddlConnection);
        // Assert.assertTrue(checkRunningDDL(tddlConnection));
        if (!checkRunningDDL(tddlConnection)) {
            return;
        }
        // rollback
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("TERMINATE REBALANCE %s", jobId));

        // check
        Assert.assertTrue(waitDDLJobFinish(tddlConnection));

        Assert.assertTrue(getDDLPlanStats(tddlConnection, jobId).equalsIgnoreCase("SUCCESS"));
    }

    public void testResumeRebalanceWithDataSchedule() throws Exception {
        // create table
        for (int i = 0; i < TABLE_COUNT; ++i) {
            String tbName = TABLE_PREFIX + i;
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(DROP_TABLE_SQL, tbName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_SQL, tbName));
        }

        String originalTableName = "multiple_pk_table3";
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(d int, a int NOT NULL AUTO_INCREMENT,b char(16), c varchar(32), PRIMARY KEY(c, a, b)"
            + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 4;
        int eachPartRows = 40960;
        String rebalanceJob = "schedule rebalance table %s shuffle_data_dist=1";

        prepareData(tddlConnection, DATABASE_NAME, originalTableName, eachPartRows, createTableStmt,
            partNum, DataManipulateUtil.TABLE_TYPE.PARTITION_TABLE);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(rebalanceJob, originalTableName));

        // check
        Thread.sleep(10500);
        Long jobId = getDDLJobId(tddlConnection);
        // Assert.assertTrue(checkRunningDDL(tddlConnection));
        if (!checkRunningDDL(tddlConnection)) {
            return;
        }
        Assert.assertTrue(jobId != -1L);

        // RESUME REBALANCE
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("RESUME REBALANCE %s", jobId));

        // check
        Assert.assertTrue(waitDDLJobFinish(tddlConnection));

        Assert.assertTrue(getDDLPlanStats(tddlConnection, jobId).equalsIgnoreCase("EXECUTING"));

        // check
        Thread.sleep(10500);
        jobId = getDDLJobId(tddlConnection);
        // Assert.assertTrue(checkRunningDDL(tddlConnection));
        if (!checkRunningDDL(tddlConnection)) {
            return;
        }
        // rollback
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("TERMINATE REBALANCE %s", jobId));

        // check
        Assert.assertTrue(waitDDLJobFinish(tddlConnection));

        Assert.assertTrue(getDDLPlanStats(tddlConnection, jobId).equalsIgnoreCase("SUCCESS"));
    }

    @Test
    public void testResumeRebalance() throws SQLException, InterruptedException {
        // create table
        for (int i = 0; i < TABLE_COUNT; ++i) {
            String tbName = TABLE_PREFIX + i;
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(DROP_TABLE_SQL, tbName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_SQL, tbName));
        }

        String command = prepareDrainNodeCommand(tddlConnection);
        JdbcUtil.executeUpdateSuccess(tddlConnection, command + " async=false");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "rebalance database");

        Long jobId = getDDLJobId(tddlConnection);
        // Assert.assertTrue(checkRunningDDL(tddlConnection));
        if (!checkRunningDDL(tddlConnection)) {
            return;
        }
        Assert.assertTrue(jobId != -1L);

        // RESUME REBALANCE
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("RESUME REBALANCE %s", jobId));

        // check
        Assert.assertTrue(waitDDLJobFinish(tddlConnection));
    }

    @Test
    public void testResumeRebalanceFail() throws SQLException, InterruptedException {

        String command = "resume rebalance all";
        String errorMsg = "Operation on multi ddl jobs is not allowed";
        JdbcUtil.executeUpdateFailed(tddlConnection, command, errorMsg);

        command = "resume rebalance 123,234";
        JdbcUtil.executeUpdateFailed(tddlConnection, command, errorMsg);
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
            if (rs.getString("GROUP").equalsIgnoreCase("TERMINATEREBALANCEAUTOTEST_P00001_GROUP")) {
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

    private static String prepareAutoDbCommands(Connection connection) throws SQLException {
        Set<String> instIds = new HashSet<>();
        String curInstId = null;

        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);
        sql = String.format(SELECT_FROM_TABLE_DETAIL, DATABASE_NAME, TABLE_NAME, MOVE_PARTITION_GROUPS);

        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (rs.next()) {
            curInstId = rs.getString("STORAGE_INST_ID");
        } else {
            throw new RuntimeException(
                String.format("not find database table %s.%s", DATABASE_NAME, TABLE_NAME));
        }
        rs.close();

        sql = String.format(SHOW_DS, DATABASE_NAME);
        rs = JdbcUtil.executeQuery(sql, connection);
        while (rs.next()) {
            if (!curInstId.equalsIgnoreCase(rs.getString("STORAGE_INST_ID"))) {
                instIds.add(rs.getString("STORAGE_INST_ID"));
            }
        }
        rs.close();

        if (!instIds.isEmpty()) {
            // move partition p1
            return String.format(MOVE_PARTITION_COMMAND, TABLE_NAME, MOVE_PARTITION_GROUPS, instIds.iterator().next());
        }
        return null;
    }

    void doReCreateDatabase() {
        doClearDatabase();
        String tddlSql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "create database " + DATABASE_NAME + " partition_mode = 'auto'";
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
