package com.alibaba.polardbx.qatest.ddl.datamigration.balancer.control;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PauseRebalanceAutoTest extends DDLBaseNewDBTestCase {
    private static final String SLOW_HINT = "GSI_DEBUG=\"slow\"";
    private static final String ASYNC_DDL_HINT = "ENABLE_ASYNC_DDL=true, PURE_ASYNC_DDL_MODE=true";
    private static final String ENABLE_CHANGESET_HINT = "CN_ENABLE_CHANGESET=%s";
    private static final String DROP_DB_HINT = "ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE=true";

    private static final int TABLE_COUNT = 4;
    static private final String DATABASE_NAME = "PauseRebalanceAutoTest";
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

    public PauseRebalanceAutoTest(boolean enableChangeSet) {
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
    public void testPauseRebalanceAndContinue() throws SQLException, InterruptedException {
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
        Thread.sleep(3000);
        Assert.assertTrue(!isTableAllPublic(tddlConnection));

        // pause
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("PAUSE REBALANCE %s", jobId));

        // check
        int count = 0;
        while (!checkPauseDDL(tddlConnection) && count < 10) {
            Thread.sleep(1000);
            count++;
        }
        Assert.assertTrue(isTableAllPublic(tddlConnection));
        Assert.assertTrue(checkPauseDDL(tddlConnection));

        // continue
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("CONTINUE REBALANCE %s", jobId));
    }

    @Test
    public void testPauseRebalanceAndRollback() throws SQLException, InterruptedException {
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
        Thread.sleep(3000);
        Assert.assertTrue(!isTableAllPublic(tddlConnection));

        // pause
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("PAUSE REBALANCE %s", jobId));

        // check
        int count = 0;
        while (!checkPauseDDL(tddlConnection) && count < 10) {
            Thread.sleep(1000);
            count++;
        }
        Assert.assertTrue(isTableAllPublic(tddlConnection));
        Assert.assertTrue(checkPauseDDL(tddlConnection));

        // rollback
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("TERMINATE REBALANCE %s", jobId));
    }

    private static boolean checkPauseDDL(Connection connection) throws SQLException {
        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);

        sql = "show ddl";
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (rs.next()) {
            String ddlState = rs.getString("STATE");
            String phyDdlProgress = rs.getString("CURRENT_PHY_DDL_PROGRESS");
            rs.close();
            return ddlState.equalsIgnoreCase("paused") && phyDdlProgress.equalsIgnoreCase("0%");
        }
        rs.close();
        return false;
    }

    private static boolean isTableAllPublic(Connection connection) throws SQLException {
        List<String> statusList = new ArrayList<>();
        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);

        sql = "show table replicate status";
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        while (rs.next()) {
            statusList.add(rs.getString("REPLICATE_STATUS"));
        }
        rs.close();

        for (String status : statusList) {
            if (!StringUtils.equalsIgnoreCase(status, "PUBLIC")) {
                return false;
            }
        }

        return true;
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
