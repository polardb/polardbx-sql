package com.alibaba.polardbx.qatest.ddl.auto.movepartition;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MovePartitionDdlTest extends MovePartitionBaseTest {
    static private final String DATABASE_NAME = "MovePartitionDdlTest";
    private static final String TABLE_NAME = "tbl";
    private static final String PARTITION_GROUP = "p3";
    private static final String MOVE_PARTITION_GROUPS = "p1";
    private static final String NEW_PARTITION_GROUP = "p4,p5";
    private static final String SHOW_DS = "show ds where db='%s'";
    private static final String MOVE_PARTITION_COMMAND = "alter table %s move partitions %s to '%s'";
    private static final String SPLIT_PARTITION_COMMAND = "alter table %s split partition %s into %s";
    private static final String MERGE_PARTITION_COMMAND = "alter table %s merge partitions %s to %s";

    private static final String SELECT_FROM_TABLE_DETAIL =
        "select storage_inst_id,table_group_name from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'";

    private static final String NEW_PARTITION_GROUPS_DEF = "(\n"
        + "  PARTITION p4 VALUES LESS THAN (6148914691236517205),\n"
        + "  PARTITION p5 VALUES LESS THAN (9223372036854775807)\n"
        + ")";

    private static final String CREATE_TABLE_SQL =
        "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP) ";

    private static final String INSERT_SQL = "insert into `%s` (b, c) values (1, now())";

    @Before
    public void before() {
        doReCreateDatabase();
    }

    public MovePartitionDdlTest() {
        super(DATABASE_NAME);
    }

    @Test
    public void alterTableGroupTest() throws Exception {
        // create table
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_SQL, TABLE_NAME));

        List<String> autoDbMoveCommands = prepareAutoDbCommands(tddlConnection);
        if (GeneralUtil.isEmpty(autoDbMoveCommands)) {
            return;
        }

        System.out.printf("%s begin to execute alter tablegroup task%n", LocalTime.now().toString());
        for (String command : autoDbMoveCommands) {
            System.out.printf("%s begin to execute command:[%s]%n", LocalTime.now().toString(), command);

            String insertDmlSql = String.format(INSERT_SQL, TABLE_NAME);
            doInsertWhileDDL(insertDmlSql, 2, new DDLRequest() {
                @Override
                public void executeDdl() {
                    try {
                        Connection conn = tddlConnection;
                        String useDbSql = String.format("use %s;", DATABASE_NAME);
                        try (Statement stmt = conn.createStatement()) {
                            stmt.execute(useDbSql);
                        } catch (Throwable ex) {
                            ex.printStackTrace();
                        }

                        try (Statement stmt = conn.createStatement()) {
                            stmt.execute(command);
                        } catch (Throwable ex) {
                            ex.printStackTrace();
                        }
                    } catch (Throwable ex) {
                        ex.printStackTrace();
                    }
                }
            });

            System.out.printf("%s command:[%s] finish%n", LocalTime.now().toString(), command);
        }
        System.out.printf("%s alter tablegroup task finish%n", LocalTime.now().toString());
    }

    private static List<String> prepareAutoDbCommands(Connection connection) throws SQLException {
        List<String> commands = new ArrayList<>();
        Set<String> instIds = new HashSet<>();
        String curInstId = null;

        String sql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(connection, sql);
        sql = String.format(SELECT_FROM_TABLE_DETAIL, DATABASE_NAME, TABLE_NAME, PARTITION_GROUP);

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
            // move partition p3
            commands.add(
                String.format(MOVE_PARTITION_COMMAND, TABLE_NAME, PARTITION_GROUP, instIds.iterator().next()));
            commands.add(String.format(MOVE_PARTITION_COMMAND, TABLE_NAME, PARTITION_GROUP, curInstId));

            // move partition p1
            commands.add(
                String.format(MOVE_PARTITION_COMMAND, TABLE_NAME, MOVE_PARTITION_GROUPS, instIds.iterator().next()));
            commands.add(String.format(MOVE_PARTITION_COMMAND, TABLE_NAME, MOVE_PARTITION_GROUPS, curInstId));

            // split and merge partition p3
            commands.add(
                String.format(SPLIT_PARTITION_COMMAND, TABLE_NAME, PARTITION_GROUP, NEW_PARTITION_GROUPS_DEF));
            commands.add(String.format(MERGE_PARTITION_COMMAND, TABLE_NAME, NEW_PARTITION_GROUP, PARTITION_GROUP));
        }
        return commands;
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
        String tddlSql = "drop database if exists " + DATABASE_NAME;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }
}
