/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.sharding.scaleout;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssertWithDiffSql;

/**
 * @author luoyanxin
 */
public abstract class ScaleOutBaseTest extends AsyncDDLBaseNewDBTestCase {

    protected String logicalDatabase;
    protected String metaDb;
    protected List<Pair<String, Integer>> groupNames = new ArrayList<>();
    protected List<String> finalTableStatus;
    protected String sourceGroupKey = StringUtils.EMPTY;
    protected Set<String> sourceGroupKeys = new TreeSet<>(String::compareToIgnoreCase);
    protected Map<String, List<String>> storageGroupInfo = new HashMap<>();
    private String originUseDbName;
    private String originSqlMode;
    protected boolean needAutoDropDbAfterTest = true;
    protected boolean printExecutedSqlLog = false;
    protected String partMode = " partition_mode='sharding'";

    public ScaleOutBaseTest(String logicalDatabase, String metaDb, List<String> finalTableStatus) {
        this.logicalDatabase = logicalDatabase;
        this.metaDb = metaDb;
        this.finalTableStatus = finalTableStatus;
    }

    public ScaleOutBaseTest(String logicalDatabase, String metaDb, List<String> finalTableStatus,
                            boolean needAutoDropDbAfterTest) {
        this(logicalDatabase, metaDb, finalTableStatus);
        this.needAutoDropDbAfterTest = needAutoDropDbAfterTest;
    }

    public void setUp(boolean createTableBeforeMoveDatabase, List<String> createTableDefinition,
                      boolean recreateDB) {
        if (recreateDB) {
            reCreateDatabase();
        }
        String tddlSql = "use " + logicalDatabase;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "show ds where db='" + logicalDatabase + "' order by group asc";
        storageGroupInfo.clear();
        sourceGroupKeys.clear();
        groupNames.clear();
        List<String> storageIDs = new ArrayList<>();

        try {
            PreparedStatement stmt = JdbcUtil.preparedStatement(tddlSql, tddlConnection);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String sid = rs.getString("STORAGE_INST_ID");
                String groupName = rs.getString("GROUP");
                if (groupName.toUpperCase().endsWith("_SINGLE_GROUP") || groupName.equalsIgnoreCase("MetaDB")) {
                    continue;
                }
                int indexOfSid = -1;
                if (!storageGroupInfo.containsKey(sid)) {
                    storageGroupInfo.put(sid, new ArrayList<>());
                    storageIDs.add(sid);
                    indexOfSid = storageIDs.size() - 1;
                } else {
                    indexOfSid = storageIDs.indexOf(sid);
                }
                storageGroupInfo.get(sid).add(groupName);
                groupNames.add(new Pair<>(groupName, indexOfSid));
            }
            rs.close();
            stmt.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        if (createTableBeforeMoveDatabase) {
            for (String createTableStr : createTableDefinition) {
                JdbcUtil.executeUpdate(tddlConnection, createTableStr);
            }
        }
        assert storageIDs.size() > 1;
        if (GeneralUtil.isEmpty(finalTableStatus) || finalTableStatus.size() == 1) {
            moveOneDb(storageIDs);
        } else {
            moveMultiDbs(storageIDs);
        }

    }

    private void moveOneDb(List<String> storageIDs) {
        String tddlSql = "";
        String finalTableStat = GeneralUtil.isEmpty(finalTableStatus) ? StringUtils.EMPTY : finalTableStatus.get(0);
        if (finalTableStat.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.ABSENT.toString())) {
            return;
        }
        boolean firstVisit = true;
        for (int i = 0; i < storageIDs.size() && firstVisit; i++) {
            if (StringUtils.isEmpty(finalTableStat) || ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.toString()
                .equalsIgnoreCase(finalTableStat)) {
                tddlSql =
                    "move database /*+TDDL:CMD_EXTRA(SCALE_OUT_DEBUG=true, SHARE_STORAGE_MODE=true, SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true)*/ ";
            } else {
                tddlSql =
                    "move database /*+TDDL:CMD_EXTRA(SCALE_OUT_DEBUG=true, SHARE_STORAGE_MODE=true, SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true, SCALE_OUT_FINAL_TABLE_STATUS_DEBUG="
                        + finalTableStat + ")*/ ";

            }
            for (String sourceGK : storageGroupInfo.get(storageIDs.get(i))) {
                if (firstVisit && sourceGK.indexOf("0001_GROUP") != -1) {
                    tddlSql = tddlSql + sourceGK;
                    firstVisit = false;
                    sourceGroupKey = sourceGK;
                    //0->1,1->2,..,n-1->0
                    tddlSql = tddlSql + " to '" + storageIDs.get((i + 1) % storageIDs.size()) + "'";
                    break;// only move one database
                }
            }
        }
        Set<String> ignoreError = new HashSet<>();
        ignoreError.add("Please use SHOW DDL");
        JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, tddlSql, ignoreError);
    }

    private void moveMultiDbs(List<String> storageIDs) {
        String tddlSql;
        int k = 0;
        for (String finalTableStat : finalTableStatus) {
            if (StringUtils.isEmpty(finalTableStat) || ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC.toString()
                .equalsIgnoreCase(finalTableStat)) {
                tddlSql =
                    "move database /*+TDDL:CMD_EXTRA(SCALE_OUT_DEBUG=true, SHARE_STORAGE_MODE=true, SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true)*/ ";
            } else {
                tddlSql =
                    "move database /*+TDDL:CMD_EXTRA(SCALE_OUT_DEBUG=true, SHARE_STORAGE_MODE=true, SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true, SCALE_OUT_FINAL_TABLE_STATUS_DEBUG="
                        + finalTableStat + ")*/ ";

            }

            if (finalTableStat.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.ABSENT.toString())) {
                return;
            }
            sourceGroupKey = groupNames.get(k).getKey();
            sourceGroupKeys.add(sourceGroupKey);
            tddlSql = tddlSql + sourceGroupKey;

            //0->1,1->2,..,n-1->0

            tddlSql = tddlSql + " to '" + storageIDs.get((groupNames.get(k).getValue() + 1) % storageIDs.size()) + "'";
            Set<String> ignoreError = new HashSet<>();
            ignoreError.add("Please use SHOW DDL");
            JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, tddlSql, ignoreError);

            k++;
        }
    }

    public void executeDml(String sql) {
        String sqlWithHint = sql;
        if (printExecutedSqlLog) {
            System.out.println(LocalTime.now().toString() + ":" + sqlWithHint);
        }
        JdbcUtil.executeUpdate(tddlConnection, sqlWithHint, false, true);
    }

    public void executeDmlSuccess(String sql) {
        String sqlWithHint = sql;
        if (printExecutedSqlLog) {
            System.out.println(LocalTime.now().toString() + ":" + sqlWithHint);
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlWithHint);
    }

    protected void checkDataContentForScaleOutWrite(List<String> physicalTables, String whereClause) {

        String targetGroup = GroupInfoUtil.buildScaleOutGroupName(sourceGroupKey);
        for (String physicalTable : physicalTables) {
            String srcSql =
                String.format("/*TDDL:node='%s'*/select * from `%s` " + whereClause, sourceGroupKey, physicalTable);
            String tarSql =
                String.format("/*TDDL:node='%s'*/select * from `%s`" + whereClause, targetGroup, physicalTable);
            selectContentSameAssertWithDiffSql(srcSql, tarSql, null, tddlConnection, tddlConnection, true,
                false, true);
        }

    }

    protected Map<String, List<String>> getTableTopology(String logicalTable) {
        Map<String, List<String>> groupAndTbMap = new HashMap<>();
        // Get topology from logicalTableName
        String showTopology = String.format("show topology from %s", logicalTable);
        PreparedStatement stmt = JdbcUtil.preparedStatement(showTopology, tddlConnection);
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery();
            while (rs.next()) {
                String groupName = (String) rs.getObject(2);
                String phyTbName = (String) rs.getObject(3);
                if (groupAndTbMap.containsKey(groupName)) {
                    groupAndTbMap.get(groupName).add(phyTbName);
                } else {
                    groupAndTbMap.put(groupName, Lists.newArrayList(phyTbName));
                }
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return groupAndTbMap;
    }

    protected void reCreateDatabase() {
        reCreateDatabase(tddlConnection, logicalDatabase, mysqlConnection, metaDb, 4);
    }

    protected void reCreateDatabase(Connection tddlConnection, String targetDbName, Connection metaDbConn,
                                    String metaDbName, int shardDbCountEachInst) {
        String tddlSql =
            "/*+TDDL:cmd_extra(ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE=true)*/drop database if exists " + targetDbName;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        String createDbHint = "";
        if (shardDbCountEachInst > 0) {
            createDbHint =
                String.format("/*+TDDL({\"extra\":{\"SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT\":\"%s\"}})*/",
                    shardDbCountEachInst);
        }
        tddlSql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = createDbHint + "create database " + targetDbName + partMode;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
    }

    protected int getDataSourceCount() {
        String showTopology = String.format("show datasources ");
        PreparedStatement stmt = JdbcUtil.preparedStatement(showTopology, tddlConnection);
        ResultSet rs = null;
        int count = 0;
        try {
            rs = stmt.executeQuery();
            while (rs.next()) {
                int writeWeight = rs.getInt("WRITE_WEIGHT");
                if (writeWeight > 0) {
                    count++;
                }
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }

    protected Map<String, List<String>> getAllPhysicalTables(String[] logicalTables) {
        Map<String, List<String>> groupAndTables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String tableName : logicalTables) {
            Map<String, List<String>> groupAndPhyTablesForThisTable = getTableTopology(tableName);
            for (Map.Entry<String, List<String>> entry : groupAndPhyTablesForThisTable.entrySet()) {
                if (groupAndTables.containsKey(entry.getKey())) {
                    groupAndTables.get(entry.getKey()).addAll(entry.getValue());
                } else {
                    groupAndTables.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return groupAndTables;
    }

    protected static List<String> getTableDefinitions() {

        List<String> createTables = new ArrayList<>();

        String tableName = "test_tb_with_pk_no_uk";
        String createTable = "create table " + tableName + "(a int, b int, k int null, PRIMARY KEY (`a`)) ";
        String partitionDef = " dbpartition by hash(a) tbpartition by hash(b) tbpartitions 3";
        createTables.add(createTable + partitionDef);

        tableName = "source_tb_with_pk_no_uk";
        createTable = "create table " + tableName + "(a int, b int, k int null, PRIMARY KEY (`a`)) ";
        partitionDef = " dbpartition by hash(a) tbpartition by hash(b) tbpartitions 3";
        createTables.add(createTable + partitionDef);

        tableName = "test_tb_with_pk_uk";
        createTable =
            "create table " + tableName
                + "(a int, b int, c int, k int null,PRIMARY KEY (`a`,`b`), UNIQUE KEY u_b(`a`,`b`,`c`)) ";
        partitionDef = " dbpartition by hash(a) tbpartition by hash(b) tbpartitions 3";
        createTables.add(createTable + partitionDef);

        tableName = "source_tb_with_pk_uk";
        createTable =
            "create table " + tableName
                + "(a int, b int, c int, k int null,PRIMARY KEY (`a`,`b`), UNIQUE KEY u_b(`a`,`b`,`c`)) ";
        partitionDef = " dbpartition by hash(a) tbpartition by hash(b) tbpartitions 3";
        createTables.add(createTable + partitionDef);

        tableName = "test_brc_tb_with_pk_no_uk";
        createTable = "create table " + tableName + "(a int, b int, k int null,PRIMARY KEY (`a`)) ";
        partitionDef = " broadcast ";
        createTables.add(createTable + partitionDef);

        tableName = "source_brc_tb_with_pk_no_uk";
        createTable = "create table " + tableName + "(a int, b int, k int null,PRIMARY KEY (`a`)) ";
        partitionDef = " broadcast ";
        createTables.add(createTable + partitionDef);

        tableName = "test_brc_tb_with_pk_uk";
        createTable =
            "create table " + tableName
                + "(a int, b int, c int, k int null,PRIMARY KEY (`a`,`b`), UNIQUE KEY u_b(`a`,`b`,`c`)) ";
        partitionDef = " broadcast ";
        createTables.add(createTable + partitionDef);

        tableName = "source_brc_tb_with_pk_uk";
        createTable =
            "create table " + tableName
                + "(a int, b int, c int, k int null,PRIMARY KEY (`a`,`b`), UNIQUE KEY u_b(`a`,`b`,`c`)) ";
        partitionDef = " broadcast ";
        createTables.add(createTable + partitionDef);

        createTable = "  CREATE TABLE IF NOT EXISTS `mdb_mtb_mk1` (\n"
            + "      `pk` bigint(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
            + "      `integer_test` int(11) DEFAULT 0,\n"
            + "      `varchar_test` varchar(255) DEFAULT '$',\n"
            + "      `datetime_test` datetime DEFAULT '0000-00-00 00:00:00',\n"
            + "      `timestamp_test` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "      PRIMARY KEY (`pk`, `integer_test`)\n"
            + "  ) ENGINE = InnoDB DEFAULT CHARSET = utf8 DBPARTITION BY HASH(pk) TBPARTITION BY HASH(pk) TBPARTITIONS 4;";
        createTables.add(createTable);

        return createTables;
    }
}

class StatusInfo {
    protected final ComplexTaskMetaManager.ComplexTaskStatus moveTableStatus;
    protected final Boolean cache;

    public StatusInfo(ComplexTaskMetaManager.ComplexTaskStatus moveTableStatus, Boolean cache) {
        this.moveTableStatus = moveTableStatus;
        this.cache = cache;
    }

    public String toString() {
        return moveTableStatus.toString() + "[Plan_Cache=" + (cache != null ? String.valueOf(cache) : "None") + "]";
    }

    public ComplexTaskMetaManager.ComplexTaskStatus getMoveTableStatus() {
        return moveTableStatus;
    }

    public Boolean isCache() {
        return cache;
    }
}