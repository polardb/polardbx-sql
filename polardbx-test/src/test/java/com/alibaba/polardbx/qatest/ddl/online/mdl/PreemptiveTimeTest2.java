package com.alibaba.polardbx.qatest.ddl.online.mdl;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
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
public class PreemptiveTimeTest2 extends DDLBaseNewDBTestCase {

    private static final Logger logger = LoggerFactory.getLogger(PreemptiveTimeTest2.class);

    String databaseName = "preemptive_time_test";
    String createKeyTableWithGsiSqlStmt =
            "CREATE TABLE `%s` (\n	`a` int(11) DEFAULT NULL,\n	`b` int(11) DEFAULT NULL,\n	GLOBAL INDEX `g_i` (`b`) COVERING (`a`) PARTITION BY KEY (`b`) PARTITIONS 3 ) PARTITION BY KEY(`a`)\nPARTITIONS 8;";



    public void RunOnceFor10s(String tableName1, String tableName2, String tableGroupName, int connectionNum, String ddl1, String ddl2, String dml1, String dml2, String createTableSql1, String createTableSql2, Boolean success)
            throws ExecutionException, InterruptedException {

        List<Connection> connections = new ArrayList<>();
        for (int i = 0; i < connectionNum; i++) {
            connections.add(getPolardbxConnection());
        }
        Connection connection = connections.get(0);

        long timeDelayInMs = 10_000;
        Boolean execptedDmlSuccess = success;
        String errMsg = "";

        String dropTableSql1 = "drop table if exists " + tableName1;
        String dropTableSql2 = "drop table if exists " + tableName2;
        String useDbSql = "use " + databaseName;
        JdbcUtil.executeUpdate(connection, useDbSql);

        logger.info(dropTableSql1);
        JdbcUtil.executeUpdateSuccess(connection, dropTableSql1);
        logger.info(dropTableSql2);
        JdbcUtil.executeUpdateSuccess(connection, dropTableSql2);
        logger.info(createTableSql1);
        JdbcUtil.executeUpdateSuccess(connection, createTableSql1);
        logger.info(createTableSql2);
        JdbcUtil.executeUpdateSuccess(connection, createTableSql2);
        if (!StringUtils.isEmpty(tableGroupName)) {
            String createTableGroupSql = String.format(" create tablegroup '%s'", tableGroupName);
            logger.info(createTableGroupSql);
            JdbcUtil.executeUpdateSuccess(connection, createTableGroupSql);
            String setTableGroupSql1 = String.format("alter table `%s` set tablegroup = '%s'", tableName1, tableGroupName);
            logger.info(setTableGroupSql1);
            JdbcUtil.executeUpdateSuccess(connection, setTableGroupSql1);
            String setTableGroupSql2 = String.format("alter table `%s` set tablegroup = '%s'", tableName2, tableGroupName);
            logger.info(setTableGroupSql2);
            JdbcUtil.executeUpdateSuccess(connection, setTableGroupSql2);
        }

        runTestCase(logger, connections, databaseName, tableName1, connectionNum, createTableSql1 + createTableSql2, ddl1, dml1, timeDelayInMs, execptedDmlSuccess, errMsg);

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


    @Before
    public void setUpTestcase() throws SQLException {
        try(Connection connection= getPolardbxConnection())  {
            JdbcUtil.executeUpdate(connection, "drop database if exists " + databaseName);
            JdbcUtil.executeUpdate(connection, "create database if not exists " + databaseName + " mode = auto");
        }
    }
}
