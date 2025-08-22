package com.alibaba.polardbx.qatest.ddl.auto.dal;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

@NotThreadSafe
public class CheckTableTest extends DDLBaseNewDBTestCase {

    private static final Logger logger = LoggerFactory.getLogger(CheckTableTest.class);

    String databaseName = "check_table_test";

    int groupNum;

    enum ErrorType {
        DROP_LOCAL_INDEX, DROP_GLOBAL_INDEX, DROP_PRIMARY_TABLE, MODIFY_LOCAL_INDEX, MODIFY_GLOBAL_INDEX,
        MODIFY_PRIMARY_TABLE, REBUILD_LOCAL_INDEX_WITHOUT_GPP
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
            case REBUILD_LOCAL_INDEX_WITHOUT_GPP:
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

    public static Pair<Integer, String> getFullObjectName(Connection connection, String tableName, String objectName,
                                                          int index) {
        String fetchNameSql = String.format("show full create table %s", objectName);
        ResultSet resultSet1 = JdbcUtil.executeQuery(fetchNameSql, connection);
        String fullTableName = JdbcUtil.getAllResult(resultSet1).get(0).get(0).toString();
        String fetchTopology = String.format("show topology %s", fullTableName);
        ResultSet resultSet2 = JdbcUtil.executeQuery(fetchTopology, connection);
        List<Object> result =
            JdbcUtil.getAllResult(resultSet2).stream().filter(
                    o -> o.get(2).toString().endsWith(String.valueOf(index)) || o.get(0).toString()
                        .equalsIgnoreCase(String.valueOf(index)))
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

    public static Pair<Integer, String> getFullGsiName(Connection connection, String tableName, String objectName,
                                                       int index) throws SQLException {
        String fetchNameSql = String.format("show global index from  %s", tableName);
        ResultSet resultSet1 = JdbcUtil.executeQuery(fetchNameSql, connection);
        String fullTableName = "";
        while (resultSet1.next()) {
            String keyName = resultSet1.getString("KEY_NAME");
            if (keyName.startsWith(objectName)) {
                fullTableName = keyName;
            }
        }
        String fetchTopology = String.format("show topology %s", fullTableName);
        ResultSet resultSet2 = JdbcUtil.executeQuery(fetchTopology, connection);
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

    public void injectError(Connection connection, String databaseName, String tableName,
                            ErrorInjection errorInjection) {
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
        case REBUILD_LOCAL_INDEX_WITHOUT_GPP:
            objectInfo = getFullObjectName(connection, tableName, tableName, errorInjection.index);
            break;
        default:
            break;
        }
        if (objectInfo != null) {
            groupIndex = objectInfo.getKey();
            fullObjectName = objectInfo.getValue();
        }
        if (errorInjection.errorType == ErrorType.REBUILD_LOCAL_INDEX_WITHOUT_GPP) {
            Connection connection1 =
                chooseConnection(databaseName, GroupInfoUtil.buildPhyDbName(databaseName, groupIndex, true));
            String physicalDb = GroupInfoUtil.buildPhyDbName(databaseName, groupIndex, true);
            JdbcUtil.executeQuerySuccess(connection1, "use " + physicalDb);
            for (String sqlStmt : errorInjection.sqlStmts) {
                String sql = String.format(sqlStmt, fullObjectName);
                logger.info("execute SQL on mysql:" + sql);
                JdbcUtil.executeUpdateSuccess(connection1, sql);
            }
        } else {
            String groupHint = String.format("/*+TDDL:node(%d)*/", groupIndex);
            for (String sqlStmt : errorInjection.sqlStmts) {
                String sql = String.format(groupHint + sqlStmt, fullObjectName);
                logger.info("execute SQL on mysql:" + sql);
                JdbcUtil.executeUpdateSuccess(connection, sql);
            }
        }
    }

    public Connection chooseConnection(String databaseName, String physicalDb) {
        return mysqlConnection;
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
        runTestCase(tableName, createTableSql, expectedResults, null, "");
    }

    public void runTestCase(String tableName, String createTableSql, List<List<String>> expectedResults,
                            ErrorInjection[] errorInjections) throws SQLException {
        runTestCase(tableName, createTableSql, expectedResults, errorInjections, "");

    }

    public void runTestCase(String tableName, String createTableSql, List<List<String>> expectedResults,
                            ErrorInjection[] errorInjections, String hint) throws SQLException {
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
                    injectError(tddlConnection, databaseName, tableName, errorInjections[i]);
                }
            }
            String checkTableSqlStmt = "check table `%s`.`%s`";
            String checkTableSql = String.format(hint + checkTableSqlStmt, databaseName, tableName);

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
            {"t2:Topology"}, {"t2:Columns"}, {"auto_shard_key_a:Local Index"}, {"Topology"},
            {"auto_shard_key_b:Local Index"}, {"Covering Columns"}};
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
            {"auto_shard_key_b:Local Index"},
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
            {"auto_shard_key_a:Local Index", "doesn't exist"}, {"Topology", "doesn't exist"},
            {"auto_shard_key_b:Local Index", "doesn't exist"}, {"Covering Columns"}};
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
            {"auto_shard_key_b:Local Index", "doesn't exist"},
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

    @Test
    public void testErrorCaseRebuildGppLocalIndex() throws SQLException {
        String tableName = "t7";
        String localIndexName = "auto_shard_key_a";
        String version =
            JdbcUtil.getAllResult(JdbcUtil.executeQuery("select @@version", getMysqlConnection())).get(0).get(0)
                .toString();
        Boolean is80Version = version.startsWith("8.0");
        if (!is80Version) {
            return;
        }
        String createTableSqlStmt =
            "CREATE TABLE `%s` (\n`a` int(11) DEFAULT NULL,\n`b` int(11) DEFAULT NULL, KEY `auto_shard_key_a` USING BTREE (`a`)) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\nPARTITION BY KEY(`a`)\nPARTITIONS 2";
        String createTableSql = String.format(createTableSqlStmt, tableName);
        String[][] result = {
            {"t7:Topology"}, {"t7:Columns"}, {"auto_shard_key_a:Local Index", "inconsistent gpp"}};
        List<List<String>> expectedResult =
            Arrays.stream(result).map(o -> Arrays.stream(o).collect(Collectors.toList())).collect(Collectors.toList());
        ErrorInjection[] errorInjections = {
            new ErrorInjection(
                "set opt_index_format_gpp_enabled=false;alter table `%s` add index `" + localIndexName + "`(a);set opt_index_format_gpp_enabled=true;",
                localIndexName, 0, ErrorType.REBUILD_LOCAL_INDEX_WITHOUT_GPP),};
        runTestCase(tableName, createTableSql, expectedResult, errorInjections,
            "/*+TDDL:cmd_extra(ENABLE_CHECK_GPP_FOR_LOCAL_INDEX=true)*/");
    }
}
