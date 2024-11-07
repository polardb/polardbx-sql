package com.alibaba.polardbx.qatest.ddl.auto.movepartition;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class UGsiMovePartitionTest extends DDLBaseNewDBTestCase {
    private static final String DROP_DB_HINT = "ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE=true";

    static private final String DATABASE_NAME = "UGsiMovePartitionTest";
    private static final String TABLE_NAME = "tb1";
    private static final String MOVE_PARTITION_GROUPS = "p1";
    private static final String SHOW_DS = "show ds where db='%s'";
    private static final String MOVE_PARTITION_COMMAND = "alter tablegroup by table %s move partitions %s to '%s'";

    private static final String SELECT_FROM_TABLE_DETAIL =
        "select storage_inst_id,table_group_name from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'";

    private static final String CREATE_TABLE_SQL =
        "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP) ";

    private static final String CREATE_UGSI_SQL = "alter table `%s` add unique global index uk_b(b)";

    private static final String DROP_TABLE_SQL = "drop table if exists `%s` ";

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
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
    public void testUGsiMovePartition() throws SQLException {
        // create table
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(DROP_TABLE_SQL, TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_SQL, TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_UGSI_SQL, TABLE_NAME));

        // move partitions
        String movePartitionsCommand = prepareAutoDbCommands(tddlConnection);
        if (movePartitionsCommand == null) {
            return;
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(movePartitionsCommand, TABLE_NAME));
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
