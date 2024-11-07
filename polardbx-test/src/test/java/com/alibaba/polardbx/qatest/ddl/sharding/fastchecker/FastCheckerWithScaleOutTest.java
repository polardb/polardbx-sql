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

package com.alibaba.polardbx.qatest.ddl.sharding.fastchecker;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhuqiwei.
 */

@Ignore
@NotThreadSafe
public class FastCheckerWithScaleOutTest extends FastCheckerTestBase {
    private static String scaleOutTemplate =
        "move database " +
            "/*+TDDL: CMD_EXTRA(SCALE_OUT_DEBUG=true, SHARE_STORAGE_MODE=true, PHYSICAL_BACKFILL_ENABLE=false, SCALEOUT_CHECK_AFTER_BACKFILL={0})*/"
            +
            "{1} to {2}";
    private static final String checkFinished = "show move database;";

    @Before
    public void prepareData() {
        schemaName = "FastCheckerTestDb";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + quoteSpecialName(schemaName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + quoteSpecialName(schemaName) + "mode=drds");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(srcTableTemplate, srcLogicTable, srcTbPartition));

        for (int i = 1; i <= 500; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
                .format(fullColumnInsert, srcLogicTable, i, "uuid()", "uuid()", "FLOOR(Rand() * 1000)", "now()"));
        }
    }

    @After
    public void cleanData() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + quoteSpecialName(schemaName));
    }

    private void executeScaleOut(boolean withFastChecker) {
        Map<String, Set<String>> groupAndStorage = getStorageIds(schemaName);
        Set<String> storages = new HashSet<>();
        groupAndStorage.forEach((group, sids) -> {
            sids.forEach(
                sid -> storages.add(sid)
            );
        });
        Map<String, Set<String>> groupAndTables = getTableTopology(srcLogicTable);
        String srcGroup = groupAndTables.keySet().stream().findFirst().get();
        String srcStorageId =
            groupAndStorage.get(srcGroup).stream().filter(sid -> sid.contains("master")).findAny().get();
        String dstStorageId =
            storages.stream().filter(sid -> !sid.equals(srcStorageId) && sid.contains("master")).findAny().get();

        String tddlSql = MessageFormat
            .format(scaleOutTemplate, withFastChecker ? "true" : "false", srcGroup, "\'" + dstStorageId + "\'");
        String jobId = null;
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlSql);
        waitUntilDdlJobSucceed(schemaName);
    }

    private void waitUntilDdlJobSucceed(String schemaName) {
        boolean succeed = false;
        while (!succeed) {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, checkFinished);
            int unfinishedDdlTaskCount = 0;
            try {
                while (rs.next()) {
                    String db = rs.getString("SCHEMA");
                    if (db == null || !db.equalsIgnoreCase(schemaName)) {
                        continue;
                    }
                    String progress = rs.getString("PROGRESS");
                    if (progress != null && !progress.contains("100")) {
                        unfinishedDdlTaskCount++;
                    }
                }
            } catch (SQLException e) {
                succeed = false;
                continue;
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (unfinishedDdlTaskCount == 0) {
                succeed = true;
            }
        }
    }

    @Test
    public void testWithFastChecker() {
        executeScaleOut(true);
    }

    @Test
    public void testWithoutFastChecker() {
        executeScaleOut(false);
    }

}

