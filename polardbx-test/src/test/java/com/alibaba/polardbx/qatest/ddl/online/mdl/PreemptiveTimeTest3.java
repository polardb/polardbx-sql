package com.alibaba.polardbx.qatest.ddl.online.mdl;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.alibaba.polardbx.qatest.ddl.online.mdl.PreemptiveTimeTestBase.runTestCase;

@NotThreadSafe
public class PreemptiveTimeTest3 extends DDLBaseNewDBTestCase {

    private static final Logger logger = LoggerFactory.getLogger(PreemptiveTimeTest3.class);

    String databaseName = "preemptive_time_test_for_drds";
    String createTableStmt =
        "CREATE TABLE `%s` (\n	`a` int(11) DEFAULT NULL,\n	`b` int(11) DEFAULT NULL) DBPARTITION BY HASH (`b`)  TBPARTITION BY HASH(`a`) TBPARTITIONS 3";

    public void RunTwiceFor10sAnd20s(String tableName, String tableGroupName, int connectionNum, String ddl1,
                                     String ddl2, String dml1, String dml2, String createTableSql)
        throws ExecutionException, InterruptedException {
        RunTwiceFor10sAnd30s(tableName, tableGroupName, connectionNum, ddl1, ddl2, dml1, dml2, createTableSql, false);
    }

    public void RunTwiceFor10sAnd30s(String tableName, String tableGroupName, int connectionNum, String ddl1,
                                     String ddl2, String dml1, String dml2, String createTableSql, Boolean allSuccess)
        throws ExecutionException, InterruptedException {

        List<Connection> connections = new ArrayList<>();
        for (int i = 0; i < connectionNum; i++) {
            connections.add(getPolardbxConnection());
        }
        Connection connection = connections.get(0);

        long timeDelayInMs = 10_000;
        Boolean execptedDmlSuccess = true;
        String errMsg = "";

        String dropTableSql = "drop table if exists " + tableName;
        String useDbSql = "use " + databaseName;
        JdbcUtil.executeUpdate(connection, useDbSql);

        logger.info(dropTableSql);
        JdbcUtil.executeUpdateSuccess(connection, dropTableSql);
        logger.info(createTableSql);
        JdbcUtil.executeUpdateSuccess(connection, createTableSql);
        if (!StringUtils.isEmpty(tableGroupName)) {
            String createTableGroupSql = String.format(" create tablegroup '%s'", tableGroupName);
            logger.info(createTableGroupSql);
            JdbcUtil.executeUpdateSuccess(connection, createTableGroupSql);
            String setTableGroupSql =
                String.format("alter table `%s` set tablegroup = '%s'", tableName, tableGroupName);
            logger.info(setTableGroupSql);
            JdbcUtil.executeUpdateSuccess(connection, setTableGroupSql);
        }

        runTestCase(logger, connections, databaseName, tableName, connectionNum, createTableSql, ddl1, dml1,
            timeDelayInMs, execptedDmlSuccess, errMsg);

        logger.info(" START THE NEXT ROUND TEST....");
        timeDelayInMs = 30_000;
        execptedDmlSuccess = allSuccess;
        errMsg = "Communications link failure";
        List<Connection> newConnections = new ArrayList<>();
        for (int i = 0; i < connectionNum; i++) {
            newConnections.add(getPolardbxConnection());
        }
        runTestCase(logger, newConnections, databaseName, tableName, connectionNum, createTableSql, ddl2, dml2,
            timeDelayInMs, execptedDmlSuccess, errMsg);

    }
//    @Test
//    public void testRenameTable() throws SQLException, ExecutionException, InterruptedException {
//        String tableName = "t1_rename_table";
//        String ddlStmt = " ALTER TABLE %s RENAME TO %s";
//        String ddl1 = String.format(ddlStmt, tableName, tableName + "_bak");
//        String ddl2 = String.format(ddlStmt, tableName + "_bak", tableName);
//
//        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
//        String dml1 = String.format(dmlStmt, tableName);
//        String dml2 = String.format(dmlStmt, tableName + "_bak");
//        RunTwiceFor10sAnd20s(tableName, "", ddl1, ddl2, dml1, dml2);
//    }

    @Test
    public void testMoveDatabase() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t21";
        String ddlStmt = " MOVE DATABASE %s to '%s'";
        String groupName = GroupInfoUtil.buildGroupName(databaseName, 1);
        List<String> storageInsts = DdlStateCheckUtil.getStorageList(tddlConnection);
        String storageInst1 = storageInsts.get(0);
        String storageInst2 = storageInsts.get(1);
        String ddl1 = String.format(ddlStmt, groupName, storageInst1);
        String ddl2 = String.format(ddlStmt, groupName, storageInst2);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createTableStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, "", 12, ddl1, ddl2, dml1, dml1, createTableSql);
    }

    @Test
    public void testMoveDatabaseWithoutChangeSet() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t22";
        String ddlStmt = "  MOVE DATABASE  /*+TDDL:cmd_extra(CN_ENABLE_CHANGESET=false)*/ %s to '%s'";
        String groupName = GroupInfoUtil.buildGroupName(databaseName, 1);
        List<String> storageInsts = DdlStateCheckUtil.getStorageList(tddlConnection);
        String storageInst1 = storageInsts.get(0);
        String storageInst2 = storageInsts.get(1);
        String ddl1 = String.format(ddlStmt, groupName, storageInst1);
        String ddl2 = String.format(ddlStmt, groupName, storageInst2);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createTableStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, "", 12, ddl1, ddl2, dml1, dml1, createTableSql);
    }

    @Test
    public void testMoveDatabaseRollback() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t23";
        String ddlStmt = " MOVE DATABASE /*+TDDL:cmd_extra(ROLLBACK_ON_CHECKER=true)*/ %s to '%s'";
        String groupName = GroupInfoUtil.buildGroupName(databaseName, 1);
        List<String> storageInsts = DdlStateCheckUtil.getStorageList(tddlConnection);
        String storageInst1 = storageInsts.get(0);
        String storageInst2 = storageInsts.get(1);
        String ddl1 = String.format(ddlStmt, groupName, storageInst1);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createTableStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, "", 12, ddl1, ddl1, dml1, dml1, createTableSql);
    }

    @Before
    public void setUpTestcase() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            JdbcUtil.executeUpdate(connection, "drop database if exists " + databaseName);
            JdbcUtil.executeUpdate(connection, "create database if not exists " + databaseName + " mode = drds");
        }
    }
}
