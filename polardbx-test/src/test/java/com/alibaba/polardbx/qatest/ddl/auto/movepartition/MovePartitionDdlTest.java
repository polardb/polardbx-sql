package com.alibaba.polardbx.qatest.ddl.auto.movepartition;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import net.jcip.annotations.NotThreadSafe;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@NotThreadSafe
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
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
        "create table `%s` ("
            + "`a` int(11) primary key auto_increment, "
            + "`b` int(11), "
            + "`c` timestamp DEFAULT CURRENT_TIMESTAMP, "
            + "v_a bigint GENERATED ALWAYS AS (b + 1) virtual,"
            + "v_b bigint GENERATED ALWAYS AS (c) virtual,"
            + "v_c timestamp GENERATED ALWAYS AS (c) virtual"
            + ") ";

    private static final String INSERT_SQL = "insert into `%s` (b, c) values (1, now())";

    @Before
    public void before() {
        doReCreateDatabase();
    }

    public MovePartitionDdlTest() {
        super(DATABASE_NAME);
    }

    @Test
    public void test00AlterTableGroupTest() throws Exception {
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

    @Test
    public void test01AlterTableMovePartitionToMultipleDnTest() throws Exception {
        String tableName1 = "alterTableMovePartitionTest_" + RandomUtils.getStringBetween(3, 5);
        useDb(tddlConnection, DATABASE_NAME);
        String sql = String.format("create table %s(a int, b int) partition by hash(a,b) partitions 8", tableName1);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        String tg1 = getTableGroupByTableName(tableName1, tddlConnection);
        Map<String, String> partInstIdMap = getPartInstIdMap(tableName1, tddlConnection);
        String p1Inst = partInstIdMap.get("p1");
        String p2Inst = partInstIdMap.get("p2");
        sql =
            String.format("alter tablegroup %s move partitions (%s) to '%s', (%s) to '%s', (%s) to '%s', (%s) to '%s'",
                tg1,
                "p1", p2Inst, "p2", p1Inst, "p3", p2Inst, "p4", p1Inst);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        Map<String, String> targetPartInstIdMap = getPartInstIdMap(tableName1, tddlConnection);
        Assert.assertTrue(targetPartInstIdMap.get("p1").equals(p2Inst));
        Assert.assertTrue(targetPartInstIdMap.get("p2").equals(p1Inst));
        Assert.assertTrue(targetPartInstIdMap.get("p3").equals(p2Inst));
        Assert.assertTrue(targetPartInstIdMap.get("p4").equals(p1Inst));
    }

    @Test
    public void test02AlterTableMovePartitionTest() throws Exception {
        String tableName1 = "alterTableMovePartitionTest_" + RandomUtils.getStringBetween(3, 5);
        String tableName2 = "alterTableMovePartitionTest_" + RandomUtils.getStringBetween(3, 5);
        useDb(tddlConnection, DATABASE_NAME);
        String sql = String.format("create table %s(a int, b int) partition by hash(a,b) partitions 2", tableName1);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        sql = String.format("create table %s like %s", tableName2, tableName1);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        String tg1 = getTableGroupByTableName(tableName1, tddlConnection);
        String tg2 = getTableGroupByTableName(tableName2, tddlConnection);
        Assert.assertTrue(tg1.equalsIgnoreCase(tg2));
        Map<String, String> partInstIdMap = getPartInstIdMap(tableName1, tddlConnection);
        sql = String.format("alter table %s move partitions %s to '%s'", tableName1, "p1", partInstIdMap.get("p2"));
        JdbcUtil.executeSuccess(tddlConnection, sql);
        sql = String.format("alter table %s move partitions %s to '%s'", tableName2, "p1", partInstIdMap.get("p2"));
        JdbcUtil.executeSuccess(tddlConnection, sql);
        tg1 = getTableGroupByTableName(tableName1, tddlConnection);
        tg2 = getTableGroupByTableName(tableName2, tddlConnection);
        Assert.assertTrue(tg1.equalsIgnoreCase(tg2));
    }

    @Test
    public void test03AlterTableGroupMovePartitionWithLocalPartitionTest() throws Exception {
        String tableName1 = "alterTableMovePartitionTest_" + RandomUtils.getStringBetween(3, 5);
        String tableName2 = "alterTableMovePartitionTest_" + RandomUtils.getStringBetween(3, 5);
        useDb(tddlConnection, DATABASE_NAME);
        String sql = String.format("CREATE TABLE `%s` (\n"
            + "    `id` varchar(50) NOT NULL DEFAULT '',\n"
            + "    `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '消息落库时间',\n"
            + "    `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '消息更新时间',\n"
            + "    PRIMARY KEY (`create_time`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`id`)\n"
            + "PARTITIONS 16\n"
            + "LOCAL PARTITION BY RANGE (create_time)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 6\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW();", tableName1);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        sql = String.format("create table %s like %s", tableName2, tableName1);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        Map<String, String> partInstIdMap = getPartInstIdMap(tableName1, tddlConnection);
        sql = String.format("alter tablegroup by table %s move partitions %s to '%s'", tableName1, "p1",
            partInstIdMap.get("p2"));
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    protected String getTableGroupByTableName(String tableName, Connection connection) throws SQLException {
        String sql = String.format("show full tables like '%s'", tableName);
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql)) {
            while (rs.next()) {
                String tableGroup = rs.getString("Table_group");
                if (tableGroup != null) {
                    rs.close();
                    return tableGroup;
                }
            }
        }
        return null;
    }

    @Test
    public void test99AlterTableGroupExternalSolution() throws Exception {
        String tableName1 = "alterTableMovePartitionExternalTest_" + RandomUtils.getStringBetween(3, 5);
        String tableName2 = "alterTableMovePartitionExternalTest2_" + RandomUtils.getStringBetween(3, 5);
        String tableName3 = "alterTableMovePartitionExternalTest3_" + RandomUtils.getStringBetween(3, 5);
        useDb(tddlConnection, DATABASE_NAME);
        String sql = String.format("create table %s(a int, b int) partition by hash(a,b) partitions 8", tableName1);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("create table %s(a int, b int) partition by hash(a,b) partitions 16", tableName2);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("create table %s(a int, b int) partition by hash(a,b) partitions 24", tableName3);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        String tg1 = getTableGroupByTableName(tableName1, tddlConnection);
        Map<String, String> tableToTableGroupName =
            DdlStateCheckUtil.getTableToTableGroupMap(tddlConnection, DATABASE_NAME);
        Map<String, String> table1partInstIdMap = getPartInstIdMap(tableName1, tddlConnection);
        String p1Inst = table1partInstIdMap.get("p1");
        String p2Inst = table1partInstIdMap.get("p2");
        String INSERT_SQL_STMT =
            "insert into metadb.rebalance_external_solution(`schema_name`, `storage_pool_name`, `table_group_name`, `solution`, `invisible`) values('%s', '%s', '%s', '%s', '%s')";
        Map<String, Map<String, String>> expectedPartInstIdMap = new HashMap<>();
        for (String table : tableToTableGroupName.keySet()) {
            if (tableToTableGroupName.get(table).equals(tg1)) {
                Map<String, String> partInstIdMap = getPartInstIdMap(table, tddlConnection);
                expectedPartInstIdMap.put(table, new HashMap<>(partInstIdMap));
            } else {
                String tableGroup = tableToTableGroupName.get(table);
                String insertSql = String.format(INSERT_SQL_STMT, DATABASE_NAME, "_default", tableGroup, "", 1);
                JdbcUtil.executeSuccess(tddlConnection, insertSql);

                Map<String, String> partInstIdMap = getPartInstIdMap(table, tddlConnection);
                expectedPartInstIdMap.put(table, new HashMap<>(partInstIdMap));
            }
        }
        expectedPartInstIdMap.get(tableName1).put("p1", p2Inst);
        expectedPartInstIdMap.get(tableName1).put("p2", p1Inst);
        expectedPartInstIdMap.get(tableName1).put("p3", p1Inst);
        expectedPartInstIdMap.get(tableName1).put("p4", p1Inst);
        expectedPartInstIdMap.get(tableName1).put("p5", p2Inst);
        expectedPartInstIdMap.get(tableName1).put("p6", p1Inst);
        String insertSql = String.format(INSERT_SQL_STMT, DATABASE_NAME, "_default", tg1,
            JSON.toJSONString(expectedPartInstIdMap.get(tableName1)), 0);
        JdbcUtil.executeSuccess(tddlConnection, insertSql);
        String rebalanceSql = "REBALANCE DATABASE SOLVE_LEVEL=\"EXTERNAL\"";
        JdbcUtil.executeSuccess(tddlConnection, rebalanceSql);
        Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(tddlConnection, rebalanceSql);
        DdlStateCheckUtil.waitTillDdlDone(tddlConnection, jobId, tableName1);
        for (String table : tableToTableGroupName.keySet()) {
            Map<String, String> targetPartInstIdMap = getPartInstIdMap(table, tddlConnection);
            for (String part : targetPartInstIdMap.keySet()) {
                com.alibaba.polardbx.common.utils.Assert.assertTrue(
                    targetPartInstIdMap.get(part).equals(expectedPartInstIdMap.get(table).get(part)));
            }
        }
    }

    protected Map<String, String> getPartInstIdMap(String logicalTable, Connection conn) {
        Map<String, String> partInstIdMap = new TreeMap<>(String::compareToIgnoreCase);
        String sql = String.format("show topology from %s", logicalTable);
        ResultSet rs = JdbcUtil.executeQuery(sql, conn);
        try {
            while (rs.next()) {
                String partName = rs.getString("PARTITION_NAME");
                String dnId = rs.getString("DN_ID");
                partInstIdMap.put(partName, dnId);
            }
        } catch (Exception ex) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            Assert.fail(errorMs + " \n" + ex);
        }
        return partInstIdMap;
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
