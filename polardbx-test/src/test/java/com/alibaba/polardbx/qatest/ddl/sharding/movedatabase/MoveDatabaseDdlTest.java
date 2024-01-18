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

package com.alibaba.polardbx.qatest.ddl.sharding.movedatabase;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

public class MoveDatabaseDdlTest extends MoveDatabaseBaseTest {

    static private final String primaryTableName = "moveDatabasePrimaryTable";

    static private final String dataBaseName = "MoveDatabaseDdlTest";

    //delete hint SHARE_STORAGE_MODE=true,SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true because there are always 2 dn in k8s
    public String scaleOutHint =
        "";

    public String scaleOutHint2 =
        "/*+TDDL:CMD_EXTRA("
            + "PHYSICAL_TABLE_START_SPLIT_SIZE = 100, PHYSICAL_TABLE_BACKFILL_PARALLELISM = 2, "
            + "ENABLE_SLIDE_WINDOW_BACKFILL = true, SLIDE_WINDOW_SPLIT_SIZE = 2, SLIDE_WINDOW_TIME_INTERVAL = 1000)*/";

    public String createTableSql =
        "create table `%s` (`a` int(11) primary key auto_increment, `b` int(11), `c` timestamp DEFAULT CURRENT_TIMESTAMP) "
            + "dbpartition by hash(a) tbpartition by hash(a) tbpartitions 2;";

    public String insertSql = "insert into `%s` (b, c) values (1, now())";

    private final Boolean useParallelBackfill;

    @Before
    public void before() {
        doReCreateDatabase();
        initDatasourceInfomation(dataBaseName);
    }

    @Parameterized.Parameters(name = "{index}:usePhyParallelBackfill={0}")
    public static List<Boolean> prepareDate() {
        return Lists.newArrayList(Boolean.FALSE, Boolean.TRUE);
    }

    public MoveDatabaseDdlTest(Boolean useParallelBackfill) {
        super(dataBaseName);
        this.useParallelBackfill = useParallelBackfill;
    }

    void doReCreateDatabase() {
        doClearDatabase();
        String createDbHint = "/*+TDDL({\"extra\":{\"SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT\":\"2\"}})*/";
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
    public void testScaleOutTask() {
        if (usingNewPartDb()) {
            return;
        }

        // create table
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTableSql, primaryTableName));

        try {
            for (String groupName : groupNames) {
                doScaleOutTaskAndCheckForOneGroup(groupName);
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

    private void doScaleOutTaskAndCheckForOneGroup(String groupName) throws Exception {
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

        String insertDmlSql = String.format(insertSql, primaryTableName);
        doInsertWhileDDL(insertDmlSql, 2, new DDLRequest() {

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
