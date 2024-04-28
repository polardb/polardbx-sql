package com.alibaba.polardbx.qatest.ddl.changeset;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TableGroupTest extends DDLBaseNewDBTestCase {
    private static final String AUTO_DB = "tpcc_auto";
    private static final String TABLE_NAME = "bmsql_order_line";
    private static final String PARTITION_GROUP = "p3";
    private static final String MOVE_PARTITION_GROUPS = "p1,p2,p3";
    private static final String SHOW_DS = "show ds where db='%s'";
    private static final String MOVE_PARTITION_COMMAND = "alter tablegroup %s move partitions %s to '%s'";

    private static final String SELECT_FROM_TABLE_DETAIL =
        "select storage_inst_id,table_group_name from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'";

    @Test
    public void alterTableGroupWithTPCCTest() throws SQLException {
        List<String> autoDbMoveCommands = prepareAutoDbCommands(tddlConnection);
        if (GeneralUtil.isEmpty(autoDbMoveCommands)) {
            return;
        }

        System.out.printf("%s begin to execute alter tablegroup task%n", LocalTime.now().toString());
        for (String command : autoDbMoveCommands) {
            System.out.printf("%s begin to execute command:[%s]%n", LocalTime.now().toString(), command);
            String sql = "use " + AUTO_DB;
            JdbcUtil.executeUpdate(tddlConnection, sql);
            JdbcUtil.executeUpdate(tddlConnection, command);
            System.out.printf("%s command:[%s] finish%n", LocalTime.now().toString(), command);
        }
        System.out.printf("%s alter tablegroup task finish%n", LocalTime.now().toString());
    }

    private static List<String> prepareAutoDbCommands(Connection connection) throws SQLException {
        List<String> commands = new ArrayList<>();
        Set<String> instIds = new HashSet<>();
        String curInstId = null;
        String curTableGroup = null;

        String sql = "use " + AUTO_DB;
        JdbcUtil.executeUpdate(connection, sql);
        sql = String.format(SELECT_FROM_TABLE_DETAIL, AUTO_DB, TABLE_NAME, PARTITION_GROUP);

        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (rs.next()) {
            curInstId = rs.getString("STORAGE_INST_ID");
            curTableGroup = rs.getString("TABLE_GROUP_NAME");
        } else {
            throw new RuntimeException(
                String.format("not find database table %s.%s", AUTO_DB, TABLE_NAME));
        }
        rs.close();

        sql = String.format(SHOW_DS, AUTO_DB);
        rs = JdbcUtil.executeQuery(sql, connection);
        while (rs.next()) {
            if (!curInstId.equalsIgnoreCase(rs.getString("STORAGE_INST_ID"))) {
                instIds.add(rs.getString("STORAGE_INST_ID"));
            }
        }
        rs.close();

        if (!instIds.isEmpty()) {
            // move partition p3
            commands.add(
                String.format(MOVE_PARTITION_COMMAND, curTableGroup, PARTITION_GROUP, instIds.iterator().next()));
            commands.add(String.format(MOVE_PARTITION_COMMAND, curTableGroup, PARTITION_GROUP, curInstId));

            // move partition p1,p2,p3
            commands.add(
                String.format(MOVE_PARTITION_COMMAND, curTableGroup, MOVE_PARTITION_GROUPS, instIds.iterator().next()));
            commands.add(String.format(MOVE_PARTITION_COMMAND, curTableGroup, MOVE_PARTITION_GROUPS, curInstId));
        }
        return commands;
    }
}
