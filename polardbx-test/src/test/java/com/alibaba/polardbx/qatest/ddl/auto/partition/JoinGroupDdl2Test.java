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

package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class JoinGroupDdl2Test extends PartitionTestBase {
    final static String CREATE_TABLE_GROUP = "create tablegroup %s";
    final static String CREATE_JOIN_GROUP = "create joingroup %s";
    final static String CREATE_JOIN_GROUP_IF_NOT_EXISTS = "create joingroup if not exists %s";
    final static String DROP_JOIN_GROUP = "drop joingroup %s";
    final static String DROP_JOIN_GROUP_IF_EXISTS = "drop joingroup if exists %s";
    final static String CREATE_TABLE = "create table t%d(a int) partition by hash(a) partitions %d %s";
    final static String DROP_TABLE = "drop table if exists t%d";
    final static String ALTER_JOING_GROUP_ADD_TABLES = "alter joingroup %s add tables %s";
    final static String ALTER_JOING_GROUP_REMOVE_TABLES = "alter joingroup %s remove tables %s";
    final static String CREATE_GSI = "create global index g%d on t%d(a) partition by hash(a) partitions %d %s";


    @Test
    public void testCreateDropJoinGroup() {
        String joinGroupName = "jg1";
        String sql = String.format(CREATE_JOIN_GROUP, joinGroupName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "ERR_JOIN_GROUP_ALREADY_EXISTS", null);

        sql = String.format(CREATE_JOIN_GROUP_IF_NOT_EXISTS, joinGroupName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(DROP_JOIN_GROUP, joinGroupName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "ERR_JOIN_GROUP_NOT_EXISTS", null);

        sql = String.format(DROP_JOIN_GROUP_IF_EXISTS, joinGroupName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testAlterJoinGroup() {
        String joinGroupName = "jg2";
        int tableCount = 9;
        String sql = String.format(CREATE_JOIN_GROUP, joinGroupName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        for (int i = 0; i < tableCount; i++) {
            sql = String.format(CREATE_TABLE, i, 3, "");
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
        List<String> tables = new ArrayList<>();
        Map<String, Long> oldTablesGid = new HashMap<>();
        Map<String, Long> newTablesGid = new HashMap<>();
        for (int i = 0; i < tableCount - 1; i = i + 2) {
            String tableName = "t" +i;
            tables.add(tableName);
            oldTablesGid.put(tableName, getTableGroupId(tableName));
        }
        sql = String.format(ALTER_JOING_GROUP_ADD_TABLES, joinGroupName, String.join(",",tables));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        for (int i = 0; i < tableCount - 1; i = i + 2) {
            String tableName = "t" +i;
            newTablesGid.put(tableName, getTableGroupId(tableName));
        }
        for (Map.Entry<String, Long> entry:newTablesGid.entrySet()) {
            Assert.assertFalse(entry.getValue().longValue() == oldTablesGid.get(entry.getKey()).longValue());
        }
        int k=0;
        sql = String.format(CREATE_GSI, k, k, 3, "");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        k++;
        sql = String.format(CREATE_GSI, k, k, 3, "");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        for(int j=0;j<k;j++) {
            String tableName = "t" +j;
            String indexName = "g"+j;
            Assert.assertEquals(getTableGroupId(tableName).longValue(), getTableGroupId(indexName, true).longValue());
        }

        //sql = String.format(ALTER_JOING_GROUP_ADD_TABLES, joinGroupName, "g0, g1");
        //JdbcUtil.executeUpdateFailed(tddlConnection, sql, "can not change the GSI's joinGroup directly");

        sql = String.format(DROP_JOIN_GROUP, joinGroupName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "ERR_JOIN_GROUP_NOT_EMPTY");

        sql = String.format(ALTER_JOING_GROUP_REMOVE_TABLES, joinGroupName, String.join(",",tables));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        for (int i = 0; i < tableCount - 1; i = i + 2) {
            String tableName = "t" +i;
            Assert.assertEquals(newTablesGid.get(tableName).longValue(), getTableGroupId(tableName).longValue());
        }
        sql = String.format(DROP_JOIN_GROUP, joinGroupName);
        JdbcUtil.executeUpdate(tddlConnection, sql);
    }

    @Test
    public void testSetJoinGroup() {
        String tableGroupName="mytg3";
        String joinGroupName = "jg3";
        int tableCount = 9;
        String sql = String.format(CREATE_JOIN_GROUP, joinGroupName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(CREATE_TABLE_GROUP, tableGroupName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        List<Long> tableGroups = new ArrayList<>();
        for (int i = 0; i < tableCount; i++) {
            sql = String.format(DROP_TABLE, i);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
        for (int i = 0; i < tableCount; i++) {
            sql = String.format(CREATE_TABLE, i, 3,
                i % 2 == 0 ? " tablegroup=" + tableGroupName : " joingroup = " + joinGroupName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            String tableName = "t" + i;
            tableGroups.add(getTableGroupId(tableName));
        }

        long tableGroup0 = tableGroups.get(0);
        long tableGroup1 = tableGroups.get(1);
        for (int i = 2; i < tableGroups.size(); i++) {
            if ((i % 2) == 0) {
                Assert.assertEquals(tableGroup0, tableGroups.get(i).longValue());
                Assert.assertFalse(tableGroup1 == tableGroups.get(i).longValue());
            } else {
                Assert.assertEquals(tableGroup1, tableGroups.get(i).longValue());
                Assert.assertFalse(tableGroup0 == tableGroups.get(i).longValue());
            }
        }

        sql = String.format(CREATE_TABLE, 20, 3, " tablegroup=" + tableGroupName + " joingroup = " + joinGroupName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertEquals(getTableGroupId("t0").longValue(), getTableGroupId("t20").longValue());
        sql = "select count(1) from information_schema.join_group where table_schema='" + tddlDatabase1 + "' and TABLE_NAME='t20'";
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        try {
            if (rs.next()) {
                Assert.assertEquals(0, rs.getLong(1));
            }
        } catch (Exception ex) {
            log.error(ex);
            Assert.fail();
        }
    }

}
