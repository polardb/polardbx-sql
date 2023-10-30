package com.alibaba.polardbx.qatest.ddl.auto.dal;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

@NotThreadSafe
public class CheckTableTest extends DDLBaseNewDBTestCase {

    private static final Logger logger = LoggerFactory.getLogger(CheckTableTest.class);

    String databaseName = "check_table_test";

    int groupNum;

    enum ErrorType {
        DROP_LOCAL_INDEX, DROP_GLOBAL_INDEX, DROP_PRIMARY_TABLE, MODIFY_LOCAL_INDEX, MODIFY_GLOBAL_INDEX,
        MODIFY_PRIMARY_TABLE
    }

    static class ErrorInjection {
        public String[] sqlStmts;
        public String objectName;
        public int index;
        public ErrorType errorType;

        public ErrorInjection(String sql, String objectName, int index, ErrorType errorType) {
            this.objectName = objectName;
            this.index = index;
            this.errorType = errorType;
            switch (this.errorType) {
            case DROP_GLOBAL_INDEX:
            case DROP_PRIMARY_TABLE:
                this.sqlStmts = new String[] {"drop table if exists `%s`"};
                break;
            case MODIFY_GLOBAL_INDEX:
            case MODIFY_PRIMARY_TABLE:
                this.sqlStmts = new String[] {"drop table if exists `%s`", sql};
                break;
            case DROP_LOCAL_INDEX:
                this.sqlStmts = new String[] {"alter table `%s` drop index " + this.objectName};
                break;
            case MODIFY_LOCAL_INDEX:
                this.sqlStmts = new String[] {"alter table `%s` drop index " + this.objectName, sql};
                break;
            }

        }

        public ErrorInjection(String objectName, int index, ErrorType errorType) {
            this.objectName = objectName;
            this.index = index;
            this.errorType = errorType;
            switch (this.errorType) {
            case DROP_GLOBAL_INDEX:
            case DROP_PRIMARY_TABLE:
                this.sqlStmts = new String[] {"drop table if exists `%s`"};
                break;
            case MODIFY_GLOBAL_INDEX:
            case MODIFY_PRIMARY_TABLE:
                this.sqlStmts = new String[] {"drop table if exists `%s`", ""};
                break;
            case DROP_LOCAL_INDEX:
                this.sqlStmts = new String[] {"alter table `%s` drop index " + this.objectName};
                break;
            case MODIFY_LOCAL_INDEX:
                this.sqlStmts = new String[] {"alter table `%s` drop index " + this.objectName, ""};
                break;
            }
        }
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

    public void injectError(Connection connection, String tableName, ErrorInjection errorInjection) {
        Pair<Integer, String> objectInfo = null;
        String fullObjectName = tableName;
        int groupIndex = errorInjection.index % groupNum;
        switch (errorInjection.errorType) {
        case DROP_LOCAL_INDEX:
        case DROP_PRIMARY_TABLE:
        case MODIFY_LOCAL_INDEX:
        case MODIFY_PRIMARY_TABLE:
            objectInfo = getFullObjectName(connection, tableName, tableName, errorInjection.index);
            break;
        case DROP_GLOBAL_INDEX:
        case MODIFY_GLOBAL_INDEX:
            objectInfo = getFullObjectName(connection, tableName, errorInjection.objectName, errorInjection.index);
            break;
        default:
            break;
        }
        if (objectInfo != null) {
            groupIndex = objectInfo.getKey();
            fullObjectName = objectInfo.getValue();
        }
        String groupHint = String.format("/*+TDDL:node(%d)*/", groupIndex);
        for (String sqlStmt : errorInjection.sqlStmts) {
            String sql = groupHint + String.format(sqlStmt, fullObjectName);
            logger.info("execute SQL:" + sql);
            JdbcUtil.executeUpdateSuccess(connection, sql);
        }
    }

    public void checkRows(List<Object> result, List<String> expectedResult) {
        Assert.assertTrue(result.get(0).toString().toLowerCase().contains(expectedResult.get(0).toLowerCase()),
            "expected contains " + expectedResult.get(0) + " but get " + result);
        if (expectedResult.size() == 1) {
            Assert.assertTrue(result.get(3).toString().toLowerCase().contains("ok"), "expected ok, but get " + result);
        } else if (expectedResult.size() == 2) {
            Assert.assertTrue(result.get(3).toString().toLowerCase().contains(expectedResult.get(1).toLowerCase()),
                "expected contains " + expectedResult.get(1) + " but get " + result);
        }
        if (expectedResult.size() == 3) {
            Assert.assertTrue(result.get(3).toString().toLowerCase().contains(expectedResult.get(1).toLowerCase()),
                "expected contains " + expectedResult.get(1) + " but get " + result);
            Assert.assertTrue(result.get(2).toString().toLowerCase().contains(expectedResult.get(2).toLowerCase()),
                "expected contains " + expectedResult.get(2) + " but get " + result);
        }
    }

    public void runTestCase(String tableName, String createTableSql, List<List<String>> expectedResults)
        throws SQLException {
        runTestCase(tableName, createTableSql, expectedResults, null);
    }

    public void runTestCase(String tableName, String createTableSql, List<List<String>> expectedResults,
                            ErrorInjection[] errorInjections) throws SQLException {
        try {

            JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + databaseName);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create database if not exists " + databaseName + " mode = 'auto'");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + databaseName);
            groupNum = JdbcUtil.getAllResult(JdbcUtil.executeQuery("show node", tddlConnection)).size();
            int rowsNum = expectedResults.size();

            JdbcUtil.executeUpdate(tddlConnection, createTableSql);

            if (errorInjections != null) {
                for (int i = 0; i < errorInjections.length; i++) {
                    injectError(tddlConnection, tableName, errorInjections[i]);
                }
            }
            String checkTableSqlStmt = "check table `%s`.`%s`";
            String checkTableSql = String.format(checkTableSqlStmt, databaseName, tableName);

            ResultSet resultSet = JdbcUtil.executeQuery(checkTableSql, tddlConnection);
            List<List<Object>> result = JdbcUtil.getAllResult(resultSet);
            Assert.assertTrue(result.size() == rowsNum,
                "In original schema, expected " + rowsNum + " rows, but get " + result);

            JdbcUtil.executeQuery("use polardbx;", tddlConnection);
            resultSet = JdbcUtil.executeQuery(checkTableSql, tddlConnection);
            result = JdbcUtil.getAllResult(resultSet);
            Assert.assertTrue(result.size() == rowsNum,
                "In default schema, expected " + rowsNum + " rows, but get " + result);
            for (int i = 0; i < rowsNum; i++) {
                checkRows(result.get(i), expectedResults.get(i));
            }
        } catch (Exception e) {
            throw e;
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + databaseName);
        }
    }

    @Test
    public void testNormalCase() throws SQLException {
        String tableName = "t1";
        String createTableSqlStmt = "create table `%s`(a int, b int) partition by hash(a) partitions 32";
        String createTableSql = String.format(createTableSqlStmt, tableName);
        String[][] result = {
            {"t1:Topology"}, {"t1:Columns"}, {"auto_shard_key_a:Local Index"}};
        List<List<String>> expectedResult =
            Arrays.stream(result).map(o -> Arrays.stream(o).collect(Collectors.toList())).collect(Collectors.toList());

        runTestCase(tableName, createTableSql, expectedResult);
    }

    @Test
    public void testGsiCase() throws SQLException {
        String tableName = "t2";
        String createTableSqlStmt =
            "CREATE TABLE `%s` (\n	`a` int(11) DEFAULT NULL,\n	`b` int(11) DEFAULT NULL,\n	GLOBAL INDEX `g_i` (`b`) COVERING (`a`) PARTITION BY KEY (`b`) PARTITIONS 3 ) PARTITION BY KEY(`a`)\nPARTITIONS 8";
        String createTableSql = String.format(createTableSqlStmt, tableName);
        String[][] result = {
            {"t2:Topology"}, {"t2:Columns"}, {"auto_shard_key_a:Local Index"}, {"Topology"}, {"Covering Columns"}};
        List<List<String>> expectedResult =
            Arrays.stream(result).map(o -> Arrays.stream(o).collect(Collectors.toList())).collect(Collectors.toList());

        runTestCase(tableName, createTableSql, expectedResult);
    }

    @Test
    public void testGsiAndLocalIndexCase() throws SQLException {
        String tableName = "t3";
        String createTableSqlStmt =
            "CREATE TABLE `%s` (\n`a` int(11) DEFAULT NULL,\n`b` int(11) DEFAULT NULL,\nGLOBAL INDEX `g_i` (`b`) COVERING (`a`) PARTITION BY KEY (`b`) PARTITIONS 3,\nKEY `auto_shard_key_a` USING BTREE (`a`),\nKEY `i_b` (`b`)\n) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\nPARTITION BY KEY(`a`)\nPARTITIONS 8";
        String createTableSql = String.format(createTableSqlStmt, tableName);
        String[][] result = {
            {"t3:Topology"}, {"t3:Columns"}, {"auto_shard_key_a:Local Index"}, {"i_b:Local Index"}, {"Topology"},
            {"Covering Columns"}};
        List<List<String>> expectedResult =
            Arrays.stream(result).map(o -> Arrays.stream(o).collect(Collectors.toList())).collect(Collectors.toList());

        runTestCase(tableName, createTableSql, expectedResult);
    }

    @Test
    public void testErrorCaseDropPhysicalTable() throws SQLException {
        String tableName = "t4";
        String localIndexName = "auto_shard_key_a";
        String gsiName = "g_i_4";
        String createTableSqlStmt =
            "CREATE TABLE `%s` (\n`a` int(11) DEFAULT NULL,\n`b` int(11) DEFAULT NULL,\nGLOBAL INDEX `g_i_4` (`b`) COVERING (`a`) PARTITION BY KEY (`b`) PARTITIONS 8,\nKEY `auto_shard_key_a` USING BTREE (`a`)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\nPARTITION BY KEY(`a`)\nPARTITIONS 8";
        String createTableSql = String.format(createTableSqlStmt, tableName);
        String[][] result = {
            {"t4:Topology", "doesn't exist"}, {"t4:Columns"}, {"auto_shard_key_a:Local Index", "doesn't exist"},
            {"auto_shard_key_a:Local Index", "doesn't exist"}, {"Topology", "doesn't exist"}, {"Covering Columns"}};
        List<List<String>> expectedResult =
            Arrays.stream(result).map(o -> Arrays.stream(o).collect(Collectors.toList())).collect(Collectors.toList());
        ErrorInjection[] errorInjections = {
            new ErrorInjection(tableName, 2, ErrorType.DROP_PRIMARY_TABLE),
            new ErrorInjection(gsiName, 4, ErrorType.DROP_GLOBAL_INDEX),
            new ErrorInjection(localIndexName, 6, ErrorType.DROP_LOCAL_INDEX),};
        runTestCase(tableName, createTableSql, expectedResult, errorInjections);
    }

    @Test
    public void testErrorCaseModifyPhysicalTable() throws SQLException {
        String tableName = "t5";
        String localIndexName = "auto_shard_key_a";
        String gsiName = "g_i_5";
        String createTableSqlStmt =
            "CREATE TABLE `%s` (\n`a` int(11) DEFAULT NULL,\n`b` int(11) DEFAULT NULL,\nGLOBAL INDEX `g_i_5` (`b`) COVERING (`a`) PARTITION BY KEY (`b`) PARTITIONS 8,\nKEY `auto_shard_key_a` USING BTREE (`a`)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\nPARTITION BY KEY(`a`)\nPARTITIONS 8";
        String createTableSql = String.format(createTableSqlStmt, tableName);
        String[][] result = {
            {"t5:Topology", "find incorrect columns 'x, y', and find missing columns 'a, b, _drds_implicit_id_'"},
            {"t5:Columns"}, {"auto_shard_key_a:Local Index", "for physical table"},
            {"auto_shard_key_a:Local Index", "for physical table"},
            {"Topology", "find incorrect columns 'x, y', and find missing columns 'a, b, _drds_implicit_id_'"},
            {"Covering Columns"}};
        List<List<String>> expectedResult =
            Arrays.stream(result).map(o -> Arrays.stream(o).collect(Collectors.toList())).collect(Collectors.toList());
        ErrorInjection[] errorInjections = {
            new ErrorInjection("alter table `%s` add index `" + localIndexName + "`(a, b)", localIndexName, 2,
                ErrorType.MODIFY_LOCAL_INDEX),
            new ErrorInjection("create table `%s`" + "(x varchar(8), y int)", gsiName, 4,
                ErrorType.MODIFY_GLOBAL_INDEX),
            new ErrorInjection("create table `%s`" + "(x varchar(8), y int)", tableName, 6,
                ErrorType.MODIFY_PRIMARY_TABLE),};
        runTestCase(tableName, createTableSql, expectedResult, errorInjections);
    }
}
