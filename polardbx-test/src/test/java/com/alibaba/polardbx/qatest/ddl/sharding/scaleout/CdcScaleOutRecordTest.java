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

import com.alibaba.polardbx.cdc.CdcTableUtil;
import com.alibaba.polardbx.qatest.ddl.cdc.CdcBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ziyang.lb
 **/
public class CdcScaleOutRecordTest extends CdcBaseTest {

    @Test
    public void testScaleOutRecordTest() throws SQLException, InterruptedException {
        String sql;
        String tokenHints;
        String dbName = "cdc_scaleout_test_" + System.currentTimeMillis();
        AtomicLong jobIdSeed = new AtomicLong(0);
        try (
            Statement stmt = tddlConnection.createStatement()) {

            tokenHints = buildTokenHints();
            sql = tokenHints + "drop database if exists " + dbName;
            stmt.execute(sql);
            Thread.sleep(2000);
            Assert.assertEquals(sql, getDdlRecordInfoListByToken(tokenHints).get(0).getDdlSql());

            tokenHints = buildTokenHints();
            sql = tokenHints + "create database " + dbName;
            stmt.execute(sql);
            Assert.assertEquals(sql, getDdlRecordInfoListByToken(tokenHints).get(0).getDdlSql());

            sql = "use " + dbName;
            stmt.execute(sql);

            tokenHints = buildTokenHints();
            String tableName = "t_ddl_scale_out_test";
            sql = tokenHints + String.format(CREATE_T_DDL_TEST_TABLE, tableName);
            stmt.execute(sql);
            //打标的建表语句和传入的建表语句并不完全一样，此处只演示是否是create语句
            Assert.assertTrue(
                StringUtils.startsWith(getDdlRecordInfoListByToken(tokenHints).get(0).getDdlSql(), tokenHints));
            doDml(jobIdSeed, tableName, 100);

            // Test Step
            // 1.如果是带有GSI的表，会报错: Table 't_ddl_test' is global secondary index table, which is forbidden to be modified,
            // 2.该测试语句如果报错，且报错信息是"Duplicate entry .... for key 'i_db_tb'"，需要将meta_db里的表scaleout_outline进行删除，然后重启一下tddl server，重启后会重建
            // 3.引擎内部，会根据group个数把ddl语句拆分为多个子语句，move database是异步操作，execute返回后需要加个等待
            sql = buildMoveDatabasesSql(dbName);
            if (StringUtils.isNotBlank(sql)) {
                stmt.execute(sql);
                waitScaleOutDdlFinish(dbName);
            }

            tokenHints = buildTokenHints();
            sql = tokenHints + "drop database " + dbName;
            stmt.execute(sql);
            stmt.execute("use __cdc__");
            Assert.assertEquals(sql, getDdlRecordInfoListByToken(tokenHints).get(0).getDdlSql());
        }
    }

    private String buildMoveDatabasesSql(String dbName) {
        Map<String, List<String>> storageGroupsInfo = new HashMap<>();
        List<String> groupNames = new ArrayList<>();
        Map<String, String> groupToStorageIdMap = new HashMap<>();

        String tddlSql = "use " + dbName;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "show datasources";
        try {
            PreparedStatement stmt = JdbcUtil.preparedStatement(tddlSql, tddlConnection);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String sid = rs.getString("STORAGE_INST_ID");
                int writeWeight = rs.getInt("WRITE_WEIGHT");
                String groupName = rs.getString("GROUP");
                if (writeWeight <= 0 || groupName.toUpperCase().endsWith("_SINGLE_GROUP") || groupName
                    .equalsIgnoreCase("MetaDB")) {
                    continue;
                }
                groupNames.add(groupName);
                if (!storageGroupsInfo.containsKey(sid)) {
                    storageGroupsInfo.put(sid, new ArrayList<>());
                }
                storageGroupsInfo.get(sid).add(groupName);
                groupToStorageIdMap.put(groupName, sid);
            }
            rs.close();
            stmt.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        String sql = "move database /*+TDDL:CMD_EXTRA(SCALE_OUT_DEBUG=true, SHARE_STORAGE_MODE=true)*/ %s,%s to '%s'";
        if (storageGroupsInfo.size() > 1) {
            Map.Entry<String, List<String>> first = null;
            Map.Entry<String, List<String>> second = null;
            int count = 0;
            for (Map.Entry<String, List<String>> item : storageGroupsInfo.entrySet()) {
                if (count == 0) {
                    first = item;
                }
                if (count == 1) {
                    second = item;
                    break;
                }
                count++;
            }

            return String.format(sql, first.getValue().get(0), first.getValue().get(1), second.getKey());
        }

        return "";
    }

    private void waitScaleOutDdlFinish(String dbName) throws SQLException {
        long startTime = System.currentTimeMillis();
        try (Statement stmt = tddlConnection.createStatement()) {
            while (true) {
                try (ResultSet rs = stmt
                    .executeQuery(
                        "select count(*) from __cdc__." + CdcTableUtil.CDC_DDL_RECORD_TABLE + " where schema_name = '"
                            + dbName + "' and sql_kind = 'MOVE_DATABASE'")) {
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        if (count == 1) {
                            break;
                        }
                    }
                    Thread.sleep(2000);
                    if (System.currentTimeMillis() - startTime > 60 * 1000) {
                        throw new RuntimeException("wait scale out ddl finish time out.");
                    }
                } catch (InterruptedException e) {
                    logger.info("interrupt", e);
                }
            }
        }

    }
}
