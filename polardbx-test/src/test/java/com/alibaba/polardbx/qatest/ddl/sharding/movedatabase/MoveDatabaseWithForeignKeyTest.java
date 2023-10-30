package com.alibaba.polardbx.qatest.ddl.sharding.movedatabase;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class MoveDatabaseWithForeignKeyTest extends MoveDatabaseBaseTest {

    static private final String child2 = "table1";
    static private final String child = "table2";
    static private final String parent = "table3";

    static private final String dataBaseName = "MoveDatabaseFkTest";

    //delete hint SHARE_STORAGE_MODE=true,SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true because there are always 2 dn in k8s
    public String scaleOutHint =
        " /*+TDDL:CMD_EXTRA(SHARE_STORAGE_MODE=true)*/ ";

    public String scaleOutHint2 =
        "/*+TDDL:CMD_EXTRA("
            + "PHYSICAL_TABLE_START_SPLIT_SIZE = 100, PHYSICAL_TABLE_BACKFILL_PARALLELISM = 2, "
            + "ENABLE_SLIDE_WINDOW_BACKFILL = true, SLIDE_WINDOW_SPLIT_SIZE = 2, SLIDE_WINDOW_TIME_INTERVAL = 1000, SHARE_STORAGE_MODE=true)*/";

    public String createChild2Sql =
        "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP, "
            + "CONSTRAINT FOREIGN KEY (`b`) REFERENCES %s (`b`) ON DELETE CASCADE on update CASCADE)"
            + "broadcast;";
    public String createChildSql =
        "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP, key (`b`), "
            + "CONSTRAINT FOREIGN KEY (`b`) REFERENCES %s (`b`) ON DELETE CASCADE on update CASCADE) "
            + "broadcast;";
    public String createParentSql =
        "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP, key(`b`)) "
            + "broadcast;";

    public String insertSql = "insert into `%s` (b, c) values (1, now())";
    public String insertFailSql = "insert into `%s` (b, c) values (100, now())";
    public String updateSql = "update `%s` set b = FLOOR(RAND() * 100) + 2, c = now()";

    @Before
    public void before() {
        doReCreateDatabase();
        initDatasourceInfomation(dataBaseName);
    }

    private final Boolean useParallelBackfill;

    @Parameterized.Parameters(name = "{index}:usePhyParallelBackfill={0}")
    public static List<Boolean> prepareDate() {
        return Lists.newArrayList(Boolean.FALSE, Boolean.TRUE);
    }

    public MoveDatabaseWithForeignKeyTest(Boolean useParallelBackfill) {
        super(dataBaseName);
        this.useParallelBackfill = useParallelBackfill;
    }

    void doReCreateDatabase() {
        doClearDatabase();
        String createDbHint = "/*+TDDL({\"extra\":{\"SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT\":\"4\"}})*/";
        String tddlSql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = createDbHint + "create database " + dataBaseName + " partition_mode = 'drds'";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "use " + dataBaseName;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
    }

    void doClearDatabase() {
        JdbcUtil.executeUpdate(getTddlConnection1(), "use information_schema");
        String tddlSql =
            "/*+TDDL:cmd_extra(ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE=true)*/drop database if exists " + dataBaseName;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }

    @Test
    public void testScaleOutFkTask() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        if (usingNewPartDb()) {
            return;
        }

        // create table
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createParentSql, parent));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createChildSql, child, parent));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createChild2Sql, child2, child));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, parent));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, child));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertSql, child2));

        try {
            for (String groupName : groupNames) {
                String insertFailDmlSql = String.format(insertFailSql, child2);
                doScaleOutTaskAndCheckForOneGroup(groupName, insertFailDmlSql);

                ResultSet rs =
                    JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child2));
                List<List<Object>> result = JdbcUtil.getAllResult(rs);
                assertEquals(1, result.size());
            }
            for (String groupName : groupNames) {
                String insertDmlSql = String.format(insertSql, child2);
                doScaleOutTaskAndCheckForOneGroup(groupName, insertDmlSql);

                ResultSet rs =
                    JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child2));
                List<List<Object>> result = JdbcUtil.getAllResult(rs);
                assertNotEquals(1, result.size());
            }

            for (String groupName : groupNames) {
                String updateDmlSql = String.format(updateSql, parent);
                doScaleOutTaskAndCheckForOneGroup(groupName, updateDmlSql);

                ResultSet rs =
                    JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", parent));
                org.junit.Assert.assertTrue(rs.next());
                long updateValue = rs.getLong(2);

                rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child));
                while (rs.next()) {
                    assertEquals(rs.getLong(2), updateValue);
                }

                rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format("select * from %s", child2));
                while (rs.next()) {
                    assertEquals(rs.getLong(2), updateValue);
                }
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

    private void doScaleOutTaskAndCheckForOneGroup(String groupName, String dmlSql) throws Exception {
        if (groupNames.isEmpty()) {
            throw new RuntimeException(
                String.format("no find a new storage for group[%s] to move", groupName));
        }

        // Prepare scale out task sql
        if (groupName == null) {
            groupName = groupNames.stream().findFirst().get();
        }

        String storageIdOfGroup = groupToStorageIdMap.get(groupName);

        boolean findTargetSid = false;
        String targetStorageId = "";
        for (String sid : storageIDs) {
            if (storageIdOfGroup.equalsIgnoreCase(sid)) {
                continue;
            }
            findTargetSid = true;
            targetStorageId = sid;
            break;
        }
        if (!findTargetSid) {
            throw new RuntimeException(
                String.format("no find a new storage for group[%s] to move", groupName));
        }

        String scaleOutTaskSql =
            String.format("move database %s %s to '%s';", useParallelBackfill ? scaleOutHint2 : scaleOutHint,
                groupName, targetStorageId);

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
