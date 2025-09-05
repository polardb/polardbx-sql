package com.alibaba.polardbx.qatest.ddl.online.mdl;


import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
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
import static org.junit.Assert.fail;

@NotThreadSafe
public class PreemptiveTimeTest1 extends DDLBaseNewDBTestCase {

    private static final Logger logger = LoggerFactory.getLogger(PreemptiveTimeTest1.class);

    String databaseName = "preemptive_time_test";
    String createKeyTableWithGsiSqlStmt =
            "CREATE TABLE `%s` (\n	`a` int(11) DEFAULT NULL,\n	`b` int(11) DEFAULT NULL,\n	GLOBAL INDEX `g_i` (`b`) COVERING (`a`) PARTITION BY KEY (`b`) PARTITIONS 3 ) PARTITION BY KEY(`a`)\nPARTITIONS 8";

    String createKeyTableSqlStmt =
            "CREATE TABLE `%s` (\n	`a` int(11) DEFAULT NULL,\n	`b` int(11) DEFAULT NULL) PARTITION BY KEY(`a`)\nPARTITIONS 8";

    String createRangeTableWithGsiSqlStmt =
            "CREATE TABLE `%s` (\n	`a` int(11) DEFAULT NULL,\n	`b` int(11) DEFAULT NULL,\n	GLOBAL INDEX `g_i` (`b`) COVERING (`a`) PARTITION BY KEY (`b`) PARTITIONS 3 ) PARTITION BY range(`a`) (partition p1 values less than(1000), partition p2 values less than(2000))";


    String createMoveTableStmt =
        "CREATE TABLE `%s` (\n	`a` int(11) DEFAULT NULL,\n	`b` int(11) DEFAULT NULL) PARTITION BY KEY(`a`)\nPARTITIONS 4;";

    String createRangeTableSqlStmt =
        "CREATE TABLE `%s` (\n	`a` int(11) DEFAULT NULL,\n	`b` int(11) DEFAULT NULL ) PARTITION BY range(`a`) (partition p1 values less than(1000), partition p2 values less than(2000))";



    String createListTableWithGsiSqlStmt =
            "CREATE TABLE `%s` (\n	`a` int(11) DEFAULT NULL,\n	`b` int(11) DEFAULT NULL,\n	GLOBAL INDEX `g_i` (`b`) COVERING (`a`) PARTITION BY KEY (`b`) PARTITIONS 3 ) PARTITION BY list(`a`)\n(partition p1 values in (1,2,3,4,5))";

    public void RunTwiceFor10sAnd20s(String tableName, String tableGroupName, int connectionNum, String ddl1, String ddl2, String dml1, String dml2, String createTableSql)
        throws ExecutionException, InterruptedException {
        RunTwiceFor10sAnd20s(tableName, tableGroupName, connectionNum, ddl1, ddl2, dml1, dml2, createTableSql, false);
    }

    public void RunTwiceFor10sAnd20s(String tableName, String tableGroupName, int connectionNum, String ddl1, String ddl2, String dml1, String dml2, String createTableSql, Boolean allSuccess)
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
            String setTableGroupSql = String.format("alter table `%s` set tablegroup = '%s'", tableName, tableGroupName);
            logger.info(setTableGroupSql);
            JdbcUtil.executeUpdateSuccess(connection, setTableGroupSql);
        }

        runTestCase(logger, connections, databaseName, tableName, connectionNum, createTableSql, ddl1, dml1, timeDelayInMs, execptedDmlSuccess, errMsg);

        logger.info(" START THE NEXT ROUND TEST....");
        timeDelayInMs = 20_000;
        execptedDmlSuccess = allSuccess;
        errMsg = "Communications link failure";
        List<Connection> newConnections = new ArrayList<>();
        for (int i = 0; i < connectionNum; i++) {
            newConnections.add(getPolardbxConnection());
        }
        runTestCase(logger, newConnections, databaseName, tableName, connectionNum, createTableSql, ddl2, dml2, timeDelayInMs, execptedDmlSuccess, errMsg);

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
    public void testAlterTableSetTableGroup() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t2_set_table_group";
        String ddlStmt = " ALTER TABLE %s SET TABLEGROUP = ''";
        String ddl1 = String.format(ddlStmt, tableName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createKeyTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, "", 4, ddl1, ddl1, dml1, dml1, createTableSql);
    }

    @Test
    public void testAlterTableGroupRenamePartition() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t3_rename_partition";
        String tableGroupName = "tg_rename_partition";
        String ddlStmt = " ALTER TABLEGROUP %s RENAME PARTITION p1 to p10";
        String ddl1 = String.format(ddlStmt, tableGroupName);
        ddlStmt = " ALTER TABLEGROUP %s RENAME PARTITION p10 to p1";
        String ddl2 = String.format(ddlStmt, tableGroupName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createKeyTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 4, ddl1, ddl2, dml1, dml1, createTableSql);
    }

    @Test
    public void testAlterTableGroupMovePartition() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t4_move_partition";
        String tableGroupName = "tg_move_partition";
        Connection conn = getPolardbxConnection();
        List<String> storageInsts = DdlStateCheckUtil.getStorageList(conn);
        String storageInst1 = storageInsts.get(0);
        String storageInst2 = storageInsts.get(1);
        String ddlStmt = " ALTER TABLEGROUP %s MOVE PARTITIONS p1,p2,p3 to \"%s\"";
        String ddl1 = String.format(ddlStmt, tableGroupName, storageInst1);
        String ddl2 = String.format(ddlStmt, tableGroupName, storageInst2);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createKeyTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl2, dml1, dml1, createTableSql);
    }

    @Test
    public void testAlterTableGroupMovePartitionRollback() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t5_move_partition_rollback";
        String tableGroupName = "tg_move_partition_rollback";
        Connection conn = getPolardbxConnection();
        List<String> storageInsts = DdlStateCheckUtil.getStorageList(conn);
        String storageInst1 = storageInsts.get(0);
        String storageInst2 = storageInsts.get(1);
        String ddlStmt = "/*+TDDL:cmd_extra(ROLLBACK_ON_CHECKER=true)*/ ALTER TABLEGROUP %s MOVE PARTITIONS p1,p2,p3 to \"%s\"";
        String ddl1 = String.format(ddlStmt, tableGroupName, storageInst1);
        String ddl2 = String.format(ddlStmt, tableGroupName, storageInst2);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createKeyTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl2, dml1, dml1, createTableSql);
    }

    @Test
    public void testAlterTableGroupSplitPartition() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t4_split_partition";
        String tableGroupName = "tg_split_partition";
        String ddlStmt1 = " ALTER TABLEGROUP %s split PARTITION p1";
        String ddlStmt2 = " ALTER TABLEGROUP %s split PARTITION p2 into partitions 5";
        String ddl1 = String.format(ddlStmt1, tableGroupName);
        String ddl2 = String.format(ddlStmt2, tableGroupName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createKeyTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl2, dml1, dml1, createTableSql);
    }

    @Test
    public void testAlterTableGroupSplitPartitionRollback() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t4_split_partition_rollback";
        String tableGroupName = "tg_split_partition_rollback";
        String ddlStmt1 = "/*+TDDL:cmd_extra(ROLLBACK_ON_CHECKER=true)*/ ALTER TABLEGROUP %s split PARTITION p1";
        String ddlStmt2 = "/*+TDDL:cmd_extra(ROLLBACK_ON_CHECKER=true)*/ ALTER TABLEGROUP %s split PARTITION p1 into partitions 5";
        String ddl1 = String.format(ddlStmt1, tableGroupName);
        String ddl2 = String.format(ddlStmt2, tableGroupName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createKeyTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl2, dml1, dml1, createTableSql);
    }

    @Test
    public void testAlterTableGroupMergePartition() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t4_merge_partition";
        String tableGroupName = "tg_merge_partition";
        String ddlStmt1 = " ALTER TABLEGROUP %s merge PARTITIONS p1, p2 to p12";
        String ddlStmt2 = " ALTER TABLEGROUP %s merge PARTITIONS p12,p3,p4 to p234";
        String ddl1 = String.format(ddlStmt1, tableGroupName);
        String ddl2 = String.format(ddlStmt2, tableGroupName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createKeyTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl2, dml1, dml1, createTableSql);
    }

    @Test
    public void testAlterTableGroupMergePartitionRollback() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t4_merge_partition_rollback";
        String tableGroupName = "tg_merge_partition_rollback";
        String ddlStmt1 = "/*+TDDL:cmd_extra(ROLLBACK_ON_CHECKER=true)*/ ALTER TABLEGROUP %s merge PARTITIONS p1, p2 to p12";
        String ddlStmt2 = "/*+TDDL:cmd_extra(ROLLBACK_ON_CHECKER=true)*/ ALTER TABLEGROUP %s merge PARTITIONS p2,p3,p4 to p234";
        String ddl1 = String.format(ddlStmt1, tableGroupName);
        String ddl2 = String.format(ddlStmt2, tableGroupName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createKeyTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl2, dml1, dml1, createTableSql);
    }


    @Test
    public void testAlterTableGroupAddPartition() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t4_add_partition";
        String tableGroupName = "tg_add_partition";
        String ddlStmt1 = " ALTER TABLEGROUP %s add PARTITION (partition p3 values less than(3000))";
        String ddlStmt2 = " ALTER TABLEGROUP %s add PARTITION (partition p4 values less than(4000),partition p5 values less than(5000))";
        String ddl1 = String.format(ddlStmt1, tableGroupName);
        String ddl2 = String.format(ddlStmt2, tableGroupName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createRangeTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl2, dml1, dml1, createTableSql, true);
    }

    @Test
    public void testAlterTableGroupDropPartition() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t4_drop_partition";
        String tableGroupName = "tg_drop_partition";
        String ddlStmt1 = " ALTER TABLEGROUP %s drop partition p1";
        String ddlStmt2 = " ALTER TABLEGROUP %s drop partition p2";
        String ddl1 = String.format(ddlStmt1, tableGroupName);
        String ddl2 = String.format(ddlStmt2, tableGroupName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createRangeTableSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl2, dml1, dml1, createTableSql, true);
    }

    @Test
    public void testAlterTableGroupTruncatePartition() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t4_truncate_partition";
        String tableGroupName = "tg_truncate_partition";
        String ddlStmt1 = " ALTER TABLEGROUP %s truncate PARTITION p1";
        String ddlStmt2 = " ALTER TABLEGROUP %s truncate PARTITION p2";
        String ddl1 = String.format(ddlStmt1, tableGroupName);
        String ddl2 = String.format(ddlStmt2, tableGroupName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createKeyTableSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl2, dml1, dml1, createTableSql, true);
    }

    @Test
    public void testAlterTableGroupModifyPartitionAddVal() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t4_addval_partition";
        String tableGroupName = "tg_addval_partition";
        String ddlStmt1 = " ALTER TABLEGROUP %s modify PARTITION p1 add values(20,30)";
        String ddlStmt2 = " ALTER TABLEGROUP %s modify PARTITION p1 add values(40)";
        String ddl1 = String.format(ddlStmt1, tableGroupName);
        String ddl2 = String.format(ddlStmt2, tableGroupName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createListTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl2, dml1, dml1, createTableSql);
    }

    @Test
    public void testAlterTableGroupModifyPartitionAddValRollback() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t4_addval_partition_rollback";
        String tableGroupName = "tg_addval_partition_rollback";
        String ddlStmt1 = "/*+TDDL:cmd_extra(ROLLBACK_ON_CHECKER=true)*/ ALTER TABLEGROUP %s modify PARTITION p1 add values(20,30)";
        String ddlStmt2 = "/*+TDDL:cmd_extra(ROLLBACK_ON_CHECKER=true)*/ ALTER TABLEGROUP %s modify PARTITION p1 add values(40)";
        String ddl1 = String.format(ddlStmt1, tableGroupName);
        String ddl2 = String.format(ddlStmt2, tableGroupName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName);
        String createTableSql = String.format(createListTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl2, dml1, dml1, createTableSql);
    }

    @Test
    public void testAlterTableSetTableGroupForce() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t4_settablegroup_force";
        String tableName2 = "t4_settablegroup_force2";
        String tableGroupName = "tg_set_tablegroup";
        String ddlStmt1 = "/*+TDDL:cmd_extra(ROLLBACK_ON_CHECKER=true)*/ ALTER table %s set tablegroup=%s force";
        String ddl1 = String.format(ddlStmt1, tableName2, tableGroupName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        try(Connection connection= getPolardbxConnection())  {
            JdbcUtil.executeUpdate(connection, "use " + databaseName);
            JdbcUtil.executeUpdate(connection, String.format(createRangeTableWithGsiSqlStmt, tableName2));
        }
        String dml1 = String.format(dmlStmt, tableName2);
        String createTableSql = String.format(createKeyTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl1, dml1, dml1, createTableSql);
    }

    @Test
    public void testAlterTableSetTableGroupForceRollback() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t4_settablegroup_force_rollback";
        String tableName2 = "t4_settablegroup_force_rollback2";
        String tableGroupName = "tg_set_tablegroup_rollback";
        String ddlStmt1 = "/*+TDDL:cmd_extra(ROLLBACK_ON_CHECKER=true)*/ ALTER table %s set tablegroup=%s force";
        String ddl1 = String.format(ddlStmt1, tableName2, tableGroupName);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        try(Connection connection= getPolardbxConnection())  {
            JdbcUtil.executeUpdate(connection, "use " + databaseName);
            JdbcUtil.executeUpdate(connection, String.format(createRangeTableWithGsiSqlStmt, tableName2));
        }
        String dml1 = String.format(dmlStmt, tableName2);
        String createTableSql = String.format(createKeyTableWithGsiSqlStmt, tableName);
        RunTwiceFor10sAnd20s(tableName, tableGroupName, 12, ddl1, ddl1, dml1, dml1, createTableSql);
    }

    @Test
    public void testRenameTables() throws SQLException, ExecutionException, InterruptedException {
        String tableName1 = "t11_rename_table1";
        String tableName2 = "t11_rename_table2";
        String ddlStmt = " RENAME TABLE %s to tmp,  %s to %s, tmp to %s ";
        String ddl1 = String.format(ddlStmt, tableName1, tableName2, tableName1, tableName2);
        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName1);
        String createTableSql1 = String.format(createKeyTableWithGsiSqlStmt, tableName1);
        String createTableSql2 = String.format(createKeyTableWithGsiSqlStmt, tableName2);

        try(Connection connection= getPolardbxConnection())  {
            JdbcUtil.executeUpdate(connection, "use " + databaseName);
            JdbcUtil.executeUpdate(connection, createTableSql2);
            // sequence 错开，避免主键冲突
            JdbcUtil.executeUpdate(connection, "alter table " + tableName2 + " auto_increment=10000000");
        }

        RunTwiceFor10sAnd20s(tableName1, "", 12, ddl1, ddl1, dml1, dml1, createTableSql1);
    }


    @Test
    public void testAlterTableMovePartition() throws SQLException, ExecutionException, InterruptedException {
        String tableName = "t12_move_partition";
        String tableName2 = "t12_move_partition1";
        String ddlStmt1 = "/*+TDDL:cmd_extra(ROLLBACK_ON_CHECKER=false)*/ ALTER table %s move partitions p1 to '%s';";
        List<String> dns = DdlStateCheckUtil.getStorageList(tddlConnection);
        String dn1 = dns.get(0);
        String dn2 = dns.get(1);
        String preDdl = String.format(ddlStmt1, tableName, dn2);
        String ddl1 = String.format(ddlStmt1, tableName2, dn2);
        String ddl2 = String.format(ddlStmt1, tableName2, dn1);

        String dmlStmt = " INSERT INTO %s (a, b) VALUES (1, 1)";
        String dml1 = String.format(dmlStmt, tableName2);
        String createTableSql = String.format(createMoveTableStmt, tableName);
        String createTableSql2 = String.format(createMoveTableStmt, tableName2);

        RunTwiceFor10sAnd20s(tableName, "", 12, ddl1, ddl2, dml1, dml1, createTableSql + createTableSql2 + preDdl);
    }

    @Before
    public void setUpTestcase() throws SQLException {
        try(Connection connection= getPolardbxConnection())  {
            JdbcUtil.executeUpdate(connection, "drop database if exists " + databaseName);
            JdbcUtil.executeUpdate(connection, "create database if not exists " + databaseName + " mode = auto");
        }
    }
}
