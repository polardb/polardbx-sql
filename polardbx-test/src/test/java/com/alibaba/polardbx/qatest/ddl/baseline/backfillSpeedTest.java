package com.alibaba.polardbx.qatest.ddl.baseline;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.lessThan;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
@NotThreadSafe
public class backfillSpeedTest extends DDLBaseNewDBTestCase {

    private static final String AUTO_DB = "move_db_test";
    private static final String DRDS_DB = "move_db_test_drds";
    private static final String MOVE_GROUP = "move_db_test_drds_000000_group";
    private static final String TABLE_NAME = "sbtest1";
    private static final String MOVE_PARTITION_GROUP = "p1";
    private static final String MOVE_DATABASE_COMMAND = "move database %s to '%s'";
    private static final String MOVE_PARTITION_COMMAND = "alter tablegroup %s move partitions %s to '%s'";
    private static final String SHOW_DS = "show ds where db='%s'";
    private static final String SELECT_FROM_TABLE_DETAIL =
        "select storage_inst_id,table_group_name from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'";
    private static final long MAX_EXECUTE_TIME = 60 * 60 * 1000; //60 min

    @Test
    public void backillSpeedBaseLine() throws SQLException {
        List<String> autoDbMoveCommands = prepareAutoDbCommands(tddlConnection);
        List<String> drdsDbMoveCommands = prepareDrdsDbMoveCommands(tddlConnection);
        if (GeneralUtil.isEmpty(autoDbMoveCommands) && GeneralUtil.isEmpty(drdsDbMoveCommands)) {
            return;
        }
        DateTime startTime = DateTime.now();
        System.out.println(String.format("%s begin to execute backfill task", LocalTime.now().toString()));
        for (String command : autoDbMoveCommands) {
            System.out.println(String.format("%s begin to execute command:[%s]", LocalTime.now().toString(), command));
            String sql = "use " + AUTO_DB;
            JdbcUtil.executeUpdate(tddlConnection, sql);
            JdbcUtil.executeUpdate(tddlConnection, command);
            System.out.println(String.format("%s command:[%s] finish", LocalTime.now().toString(), command));
        }
        for (String command : drdsDbMoveCommands) {
            System.out.println(String.format("%s begin to execute command:[%s]", LocalTime.now().toString(), command));
            String sql = "use " + DRDS_DB;
            JdbcUtil.executeUpdate(tddlConnection, sql);
            JdbcUtil.executeUpdate(tddlConnection, command);
            System.out.println(String.format("%s command:[%s] finish", LocalTime.now().toString(), command));
        }
        DateTime endTime = DateTime.now();
        long diff = endTime.getMillis() - startTime.getMillis();

        System.out.println(String.format("%s backfill task finish", LocalTime.now().toString()));
        Assert.assertThat(String.format("max execute time can't greater than %s sec", MAX_EXECUTE_TIME / 1000), diff,
            lessThan(MAX_EXECUTE_TIME));
    }

    private static List<String> prepareAutoDbCommands(Connection connection) throws SQLException {
        List<String> commands = new ArrayList<>();
        Set<String> instIds = new HashSet<>();
        String curInstId = null;
        String curTableGroup = null;

        String sql = String.format(SHOW_DS, AUTO_DB);
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (!rs.next()) {
            return commands;
        }

        sql = "use " + AUTO_DB;
        JdbcUtil.executeUpdate(connection, sql);
        sql = String.format(SELECT_FROM_TABLE_DETAIL, AUTO_DB, TABLE_NAME, MOVE_PARTITION_GROUP);
        rs = JdbcUtil.executeQuery(sql, connection);
        try {
            if (rs.next()) {
                curInstId = rs.getString("STORAGE_INST_ID");
                curTableGroup = rs.getString("TABLE_GROUP_NAME");
            }
        } catch (SQLException ex) {
            throw ex;
        }
        sql = String.format(SHOW_DS, AUTO_DB);
        rs = JdbcUtil.executeQuery(sql, connection);
        try {
            while (rs.next()) {
                if (!curInstId.equalsIgnoreCase(rs.getString("STORAGE_INST_ID"))) {
                    instIds.add(rs.getString("STORAGE_INST_ID"));
                }
            }
        } catch (SQLException ex) {
            throw ex;
        }
        if (!instIds.isEmpty()) {
            commands.add(
                String.format(MOVE_PARTITION_COMMAND, curTableGroup, MOVE_PARTITION_GROUP, instIds.iterator().next()));
            commands.add(String.format(MOVE_PARTITION_COMMAND, curTableGroup, MOVE_PARTITION_GROUP, curInstId));
        }
        return commands;
    }

    private static List<String> prepareDrdsDbMoveCommands(Connection connection) throws SQLException {
        List<String> commands = new ArrayList<>();
        Set<String> instIds = new HashSet<>();
        String curInstId = null;

        String sql = String.format(SHOW_DS, DRDS_DB);
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (!rs.next()) {
            return commands;
        }

        sql = "use " + DRDS_DB;
        JdbcUtil.executeUpdate(connection, sql);
        sql = String.format(SHOW_DS, DRDS_DB);
        rs = JdbcUtil.executeQuery(sql, connection);
        try {
            while (rs.next()) {
                if (MOVE_GROUP.equalsIgnoreCase(rs.getString("GROUP"))) {
                    curInstId = rs.getString("STORAGE_INST_ID");
                } else {
                    instIds.add(rs.getString("STORAGE_INST_ID"));
                }
            }
        } catch (SQLException ex) {
            throw ex;
        }
        if (!instIds.isEmpty()) {
            commands.add(String.format(MOVE_DATABASE_COMMAND, MOVE_GROUP, instIds.iterator().next()));
            commands.add(String.format(MOVE_DATABASE_COMMAND, MOVE_GROUP, curInstId));
        }
        return commands;
    }
}
