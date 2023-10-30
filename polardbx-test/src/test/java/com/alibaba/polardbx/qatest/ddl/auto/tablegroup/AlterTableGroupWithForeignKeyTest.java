package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ddl.sharding.movedatabase.MoveDatabaseBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class AlterTableGroupWithForeignKeyTest extends MoveDatabaseBaseTest {
    static private final String child2 = "table1";
    static private final String child = "table2";
    static private final String parent = "table3";

    static private final String tableGroupName = "altertablegroup_tg";
    static private final String dataBaseName = "TableGroupMovePartitionFkTest";
    private String targetDnInstId = "";

    //delete hint SHARE_STORAGE_MODE=true,SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true because there are always 2 dn in k8s
    public String scaleOutHint =
        "";

    public String scaleOutHint2 =
        "/*+TDDL:CMD_EXTRA("
            + "PHYSICAL_TABLE_START_SPLIT_SIZE = 100, PHYSICAL_TABLE_BACKFILL_PARALLELISM = 2, "
            + "ENABLE_SLIDE_WINDOW_BACKFILL = true, SLIDE_WINDOW_SPLIT_SIZE = 2, SLIDE_WINDOW_TIME_INTERVAL = 1000)*/";

    public String createChild2Sql =
        "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP, "
            + "CONSTRAINT FOREIGN KEY (`b`) REFERENCES %s (`b`) ON DELETE CASCADE on update CASCADE)"
            + "single;";
    public String createChildSql =
        "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP, key (`b`), "
            + "CONSTRAINT FOREIGN KEY (`b`) REFERENCES %s (`b`) ON DELETE CASCADE on update CASCADE) "
            + "single;";
    public String createParentSql =
        "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP, key(`b`)) "
            + "single;";

    public String insertSql = "insert into `%s` (b, c) values (1, now())";
    public String insertFailSql = "insert into `%s` (b, c) values (100, now())";
    public String updateSql = "update `%s` set b = FLOOR(RAND() * 100) + 2, c = now()";

    @Before
    public void before() {
        doReCreateDatabase();
    }

    private final Boolean useParallelBackfill;

    @Parameterized.Parameters(name = "{index}:usePhyParallelBackfill={0}")
    public static List<Boolean> prepareDate() {
        return Lists.newArrayList(Boolean.FALSE, Boolean.TRUE);
    }

    public AlterTableGroupWithForeignKeyTest(Boolean useParallelBackfill) {
        super(dataBaseName);
        this.useParallelBackfill = useParallelBackfill;
    }

    void doReCreateDatabase() {
        doClearDatabase();
        String createDbHint = "/*+TDDL({\"extra\":{\"SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT\":\"4\"}})*/";
        String tddlSql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = createDbHint + "create database " + dataBaseName + " partition_mode = 'auto'";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "use " + dataBaseName;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "create tablegroup " + tableGroupName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlSql);
    }

    void doClearDatabase() {
        JdbcUtil.executeUpdate(getTddlConnection1(), "use information_schema");
        String tddlSql =
            "/*+TDDL:cmd_extra(ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE=true)*/drop database if exists " + dataBaseName;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }

    protected List<String> getStorageInstIds() {
        String sql = String.format("show ds where db='%s'", dataBaseName);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> storageInstIds = new ArrayList<>();
        try {
            while (rs.next()) {
                storageInstIds.add(rs.getString("STORAGE_INST_ID"));
            }
        } catch (Exception ex) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            org.junit.Assert.fail(errorMs + " \n" + ex);
        }
        return storageInstIds;
    }

    protected Map<String, String> getPartInstIdMap(String logicalTable) {
        Map<String, String> partGroupMap = new TreeMap<>(String::compareToIgnoreCase);
        Map<String, String> groupInstIdMap = new TreeMap<>(String::compareToIgnoreCase);
        Map<String, String> partInstIdMap = new TreeMap<>(String::compareToIgnoreCase);
        String sql = String.format("show topology from %s", logicalTable);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        try {
            while (rs.next()) {
                String partName = rs.getString("PARTITION_NAME");
                String groupName = rs.getString("GROUP_NAME");
                partGroupMap.put(partName, groupName);
            }
        } catch (Exception ex) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            org.junit.Assert.fail(errorMs + " \n" + ex);
        }
        sql = String.format("show ds where db='%s'", dataBaseName);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        try {
            while (rs.next()) {
                String storageInstId = rs.getString("STORAGE_INST_ID");
                String groupName = rs.getString("GROUP");
                groupInstIdMap.put(groupName, storageInstId);
            }
        } catch (Exception ex) {
            String errorMs = "[Execute preparedStatement query] failed! sql is: " + sql;
            org.junit.Assert.fail(errorMs + " \n" + ex);
        }
        for (Map.Entry<String, String> entry : partGroupMap.entrySet()) {
            partInstIdMap.put(entry.getKey(), groupInstIdMap.get(entry.getValue()));
        }
        return partInstIdMap;
    }

    protected String getTargetDnInstIdForMove() {
        String targetInstId = "";

        Map<String, String> partInstIdMap = getPartInstIdMap("table3");
        String sourceInstId = partInstIdMap.get("p1");

        List<String> instIds = getStorageInstIds();
        for (String instId : instIds) {
            if (!instId.equalsIgnoreCase(sourceInstId)) {
                targetInstId = instId;
                break;
            }
        }

        return targetInstId;
    }

    @Test
    public void testScaleOutFkTask() {
        if (usingNewPartDb()) {
            return;
        }

        // create table
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createParentSql, parent));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createChildSql, child, parent));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createChild2Sql, child2, child));

        String alterTableSetTg = "alter table " + parent + " set tablegroup=" + tableGroupName + " force";
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterTableSetTg);
        alterTableSetTg = "alter table " + child + " set tablegroup=" + tableGroupName + " force";
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterTableSetTg);
        alterTableSetTg = "alter table " + child2 + " set tablegroup=" + tableGroupName + " force";
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterTableSetTg);

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, parent));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, child));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, child2));

        targetDnInstId = getTargetDnInstIdForMove();

        try {
            // insert values violate foreign key constraints
            String insertFailDmlSql = String.format(insertFailSql, child2);
            doScaleOutTaskAndCheckForOneGroup(insertFailDmlSql);

            ResultSet rs =
                JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child2));
            List<List<Object>> result = JdbcUtil.getAllResult(rs);
            assertEquals(1, result.size());

            // insert values obey foreign key constraints
            String insertDmlSql = String.format(insertSql, child2);
            doScaleOutTaskAndCheckForOneGroup(insertDmlSql);

            rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child2));
            result = JdbcUtil.getAllResult(rs);
            assertNotEquals(1, result.size());

            // update value for foreign key cascade
            String updateDmlSql = String.format(updateSql, parent);
            doScaleOutTaskAndCheckForOneGroup(updateDmlSql);

            rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", parent));
            org.junit.Assert.assertTrue(rs.next());
            long updateValue = rs.getLong(2);
            assertNotEquals(updateValue, 1);

            rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child));
            while (rs.next()) {
                assertEquals(rs.getLong(2), updateValue);
            }

            rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child2));
            while (rs.next()) {
                assertEquals(rs.getLong(2), updateValue);
            }

        } catch (Throwable ex) {
            ex.printStackTrace();
            if (!ex.getMessage().contains("Failed to get MySQL version: Unknown database ")) {
                Assert.fail(ex.getMessage());
            }

        } finally {
            doClearDatabase();
        }
    }

    private void doScaleOutTaskAndCheckForOneGroup(String dmlSql) throws Exception {
        String scaleOutTaskSql =
            String.format("alter tablegroup %s %s move partitions p1 to '%s';",
                useParallelBackfill ? scaleOutHint2 : scaleOutHint,
                tableGroupName, targetDnInstId);

        doInsertWhileDDL(dmlSql, 2, new DDLRequest() {
            @Override
            public void executeDdl() {
                try {
                    Connection conn = tddlConnection;
                    String useDbSql = String.format("use %s;", dataBaseName);
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute(useDbSql);
                    } catch (Throwable ex) {
                        ex.printStackTrace();
                    }

                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute(scaleOutTaskSql);
                    } catch (Throwable ex) {
                        ex.printStackTrace();
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        });

        System.out.print(scaleOutTaskSql);
    }
}
