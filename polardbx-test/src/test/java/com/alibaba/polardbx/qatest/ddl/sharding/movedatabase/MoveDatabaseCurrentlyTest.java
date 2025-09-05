package com.alibaba.polardbx.qatest.ddl.sharding.movedatabase;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;

public class MoveDatabaseCurrentlyTest extends MoveDatabaseBaseTest {

    static private final String primaryTableName = "moveDatabasePrimaryTable";

    static private final String dataBaseName = "MoveDatabaseCurrentlyTest";

    public String scaleOutHint = "/*+TDDL:CMD_EXTRA(PHYSICAL_BACKFILL_ENABLE=false)*/";

    public String createTableSql =
            "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP) "
                    + "dbpartition by hash(a) tbpartition by hash(a) tbpartitions 10;";

    private static final String checkDdlEngineTaskSql =
            "select * from metadb.ddl_engine_task where job_id={0} and state != ''SUCCESS'';";
    private static final String checkDdlEngineSql =
            "select state from metadb.ddl_engine where job_id={0};";

    @Before
    public void before() {
        doReCreateDatabase();
        initDatasourceInfomation(dataBaseName);
    }

    public MoveDatabaseCurrentlyTest() {
        super(dataBaseName);
    }

    void doReCreateDatabase() {
        doClearDatabase();
        String createDbHint = "/*+TDDL({\"extra\":{\"SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT\":\"2\"}})*/";
        String tddlSql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = createDbHint + "create database " + dataBaseName + " partition_mode = 'drds'";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "use " + dataBaseName;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
    }

    void doClearDatabase() {
        JdbcUtil.executeUpdate(getTddlConnection1(), "use information_schema");
        String tddlSql =
                "/*+TDDL:cmd_extra(ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE=true)*/drop database if exists " + dataBaseName;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }

    @Test
    public void testScaleOutTask() {
        if (usingNewPartDb()) {
            return;
        }

        // create table
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTableSql, primaryTableName));

        try {
            for (String groupName : groupNames) {
                if (groupName.indexOf("SINGLE_GROUP") == -1) {
                    doMoveGroupToOneNode(groupName);
                }
            }
            doRebalanceDatabase();
        } catch (Throwable ex) {
            ex.printStackTrace();
            if (!ex.getMessage().contains("Unknown database")) {
                Assert.fail(ex.getMessage());
            }

        }
    }

    private void doRebalanceDatabase() throws SQLException {
        String rebalanceSql = "/*+TDDL:cmd_extra(physical_backfill_enable=false)*/rebalance database";
        //rebalance
        JdbcUtil.executeUpdate(tddlConnection, "use " + dataBaseName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, rebalanceSql);
        String jobId = null;
        try {
            while (rs.next()) {
                jobId = rs.getString("JOB_ID");
            }
        } catch (SQLException ex) {
            assertWithMessage("语句并未按照预期执行成功:" + ex.getMessage()).fail();
        }
        if (jobId == null) {
            Assert.fail("rebalance database failed");
        }
        System.out.println(rebalanceSql);
        waitUntilDdlJobSucceed(jobId);
    }

    private void doMoveGroupToOneNode(String groupName) throws Exception {
        if (groupNames.isEmpty()) {
            throw new RuntimeException(
                    String.format("no find a new storage for group[%s] to move", groupName));
        }

        // Prepare scale out task sql
        if (groupName == null) {
            groupName = groupNames.stream().findFirst().get();
        }

        String targetStorageId = groupToStorageIdMap.values().iterator().next();

        String scaleOutTaskSql =
                String.format("move database %s %s to '%s';", scaleOutHint,
                        groupName, targetStorageId);

        Connection conn = tddlConnection;
        String useDbSql = String.format("use %s;", dataBaseName);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(useDbSql);
            stmt.execute(scaleOutTaskSql);
        } catch (Throwable ex) {
            ex.printStackTrace();
        }

        System.out.println(scaleOutTaskSql);
    }

    private void waitUntilDdlJobSucceed(String jobId) throws SQLException {
        boolean succeed = false;
        while (!succeed) {
            ResultSet rs =
                    JdbcUtil.executeQuerySuccess(tddlConnection, MessageFormat.format(checkDdlEngineTaskSql, jobId));
            int unfinishedDdlTaskCount = 0;
            try {
                if (rs.next()) {
                    unfinishedDdlTaskCount++;
                }
            } catch (SQLException e) {
                succeed = false;
                continue;
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (unfinishedDdlTaskCount == 0) {
                succeed = true;
            } else {
                rs = JdbcUtil.executeQuerySuccess(tddlConnection, MessageFormat.format(checkDdlEngineSql, jobId));
                if (rs.next()) {
                    String state = rs.getString("state");
                    if (!(state.equalsIgnoreCase("QUEUED") ||
                            state.equalsIgnoreCase("RUNNING") ||
                            state.equalsIgnoreCase("COMPLETED"))) {
                        Assert.fail("rebalance database failed with state = " + state);
                    }
                }
            }
        }
    }
}
