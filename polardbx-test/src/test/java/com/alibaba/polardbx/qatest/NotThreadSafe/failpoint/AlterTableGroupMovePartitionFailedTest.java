package com.alibaba.polardbx.qatest.NotThreadSafe.failpoint;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AlterTableGroupMovePartitionFailedTest extends DDLBaseNewDBTestCase {
    static private final String DATABASE_NAME = "MovePartitionFailedTest";

    private static final String SHOW_DS = "show ds where db='%s'";

    private static final String SELECT_FROM_TABLE_DETAIL =
        "select storage_inst_id,table_group_name from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'";

    private static final String CREATE_TABLE_SQL =
        "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP) PARTITION BY KEY(`a`) PARTITIONS 3";

    private static final String INSERT_SQL =
        "insert into `%s` (b, c) values (1, now()), (2, now()), (3, now()), (4, now())";

    private static final String ALTER_TABLE_GROUP_SQL = "alter tablegroup %s move partitions %s to '%s'";

    private static final String PARTITION_NAME = "p1";

    @Before
    public void before() {
        doReCreateDatabase();
    }

    @Test
    public void alterTableGroupWithFailPointTest() throws Exception {
        List<String> tableNames = new ArrayList<>();
        for (int i = 0; i < 4; ++i) {
            String tableName = "t" + i + RandomUtils.getStringBetween(1, 5);
            tableNames.add(tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_SQL, tableName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(INSERT_SQL, tableName));
        }

        List<String> commands = prepareCommands(tddlConnection, tableNames.get(0));

        String failPointSql = String.format("set @FP_SPECIFIED_TABLE_DROP_PHY_EXCEPTION='%s'", tableNames.get(3));
        String failPointClearSql = "set @FP_SPECIFIED_TABLE_DROP_PHY_EXCEPTION=NULL";

        for (String command : commands) {
            System.out.printf("%s begin to execute command:[%s]%n", LocalTime.now().toString(), command);

            // 异常注入
            JdbcUtil.executeUpdateSuccess(tddlConnection, failPointSql);

            // 执行命令
            JdbcUtil.executeUpdateFailed(tddlConnection, command, "FP_SPECIFIED_TABLE_DROP_PHY_EXCEPTION");

            System.out.printf("%s command:[%s] failed due to exception injection%n", LocalTime.now().toString(),
                command);

            // 检查
            Assert.assertTrue(checkTables(tddlConnection, tableNames, 4L));

            // 清除异常注入
            JdbcUtil.executeUpdateSuccess(tddlConnection, failPointClearSql);

            Long jobId = getDDLJobId(tddlConnection);

            // 回滚失败
            JdbcUtil.executeUpdateFailed(tddlConnection, String.format("ROLLBACK DDL %s", jobId),
                "cannot be rolled back");

            // 继续执行
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format("CONTINUE DDL %s", jobId));

            // 检查
            Assert.assertTrue(checkTables(tddlConnection, tableNames, 4L));

            System.out.printf("%s command:[%s] finish%n", LocalTime.now().toString(), command);
        }
    }

    private static boolean checkTables(Connection connection, List<String> tableNames, long rowCount)
        throws SQLException {
        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);

        for (String tableName : tableNames) {
            // check table
            sql = String.format("check table %s", tableName);
            try (ResultSet rs = JdbcUtil.executeQuery(sql, connection)) {
                while (rs.next()) {
                    String text = rs.getString("MSG_TEXT");
                    if (!StringUtils.equalsIgnoreCase(text, "OK")) {
                        return false;
                    }
                }
            }

            // check row count
            sql = String.format("select count(*) from %s", tableName);
            try (ResultSet rs = JdbcUtil.executeQuery(sql, connection)) {
                if (rs.next()) {
                    if (rs.getLong(1) != rowCount) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    private static List<String> prepareCommands(Connection connection, String tableName) throws SQLException {
        List<String> commands = new ArrayList<>();
        Set<String> instIds = new HashSet<>();
        String curInstId;
        String tableGroupName;

        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);

        sql = String.format(SELECT_FROM_TABLE_DETAIL, DATABASE_NAME, tableName, "p1");
        try (ResultSet rs = JdbcUtil.executeQuery(sql, connection)) {
            if (rs.next()) {
                curInstId = rs.getString("STORAGE_INST_ID");
                tableGroupName = rs.getString("TABLE_GROUP_NAME");
            } else {
                throw new RuntimeException(
                    String.format("table %s.%s not found", DATABASE_NAME, tableName));
            }
        }

        sql = String.format(SHOW_DS, DATABASE_NAME);
        try (ResultSet rs = JdbcUtil.executeQuery(sql, connection)) {
            while (rs.next()) {
                if (!curInstId.equalsIgnoreCase(rs.getString("STORAGE_INST_ID"))) {
                    instIds.add(rs.getString("STORAGE_INST_ID"));
                }
            }
        }

        Assert.assertTrue(!instIds.isEmpty());

        for (String instId : instIds) {
            // move partition p1
            commands.add(String.format(ALTER_TABLE_GROUP_SQL, tableGroupName, PARTITION_NAME, instId));
        }
        return commands;
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

    void doReCreateDatabase() {
        doClearDatabase();
        String sql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = "create database " + DATABASE_NAME + " partition_mode = 'auto'";
        JdbcUtil.executeUpdate(tddlConnection, sql);
        sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

    void doClearDatabase() {
        JdbcUtil.executeUpdate(getTddlConnection1(), "use information_schema");
        String sql = "drop database if exists " + DATABASE_NAME;
        JdbcUtil.executeUpdate(getTddlConnection1(), sql);
    }
}
