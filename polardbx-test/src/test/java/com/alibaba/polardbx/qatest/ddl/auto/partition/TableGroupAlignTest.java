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
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class TableGroupAlignTest extends PartitionTestBase {
    final static String CREATE_TABLE_GROUP = "create tablegroup %s";
    final static String DROP_TABLE_GROUP = "drop tablegroup %s";

    final static String CREATE_JOIN_GROUP = "create joingroup %s";
    final static String MOVE_PARTITIONS = "alter tablegroup %s move partitions p1,p2,p3 to '%s'";
    final static String DROP_JOIN_GROUP = "drop joingroup %s";
    final static String MERGE_GROUPS = "merge tablegroups %s into %s force";
    final static String CREATE_TABLE = "create table t%d(a int) partition by hash(a) partitions %d %s";
    final static String DROP_TABLE = "drop table if exists t%d";
    final static String ALTER_TABLE_GROUP_ADD_TABLES_FORCE = "alter tablegroup %s add tables %s force";
    final static String ALTER_TABLE_GROUP_ADD_TABLES = "alter tablegroup %s add tables %s";
    final static String ALTER_JOIN_GROUP_ADD_TABLES = "alter joingroup %s add tables %s";

    final static String ALTER_JOING_GROUP_REMOVE_TABLES = "alter joingroup %s remove tables %s";
    final static String CREATE_GSI = "create global index g%d on t%d(a) partition by hash(a) partitions %d %s";

    @Test
    public void testMergeTableGroup() {
        String tableGroup1 = "mytg1" + RandomUtils.getStringBetween(1, 5);
        String sql = String.format(CREATE_TABLE_GROUP, tableGroup1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tableGroup2 = "mytg2" + RandomUtils.getStringBetween(1, 5);
        sql = String.format(CREATE_TABLE_GROUP, tableGroup2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String joinGroupName = "jg1" + RandomUtils.getStringBetween(1, 5);
        sql = String.format(CREATE_JOIN_GROUP, joinGroupName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        int tableCount = 15;
        for (int i = 0; i < tableCount; i++) {
            sql = String.format(DROP_TABLE, i);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = String.format(CREATE_TABLE, i, 3,
                i % 3 == 0 ? " tablegroup=" + tableGroup1 :
                    (i % 3 == 1 ? " tablegroup=" + tableGroup2 : " joingroup=" + joinGroupName));
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            int rowCount = 20;
            while (rowCount > 0) {
                sql = String.format("insert into t%d values(%d)", i, rowCount);
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
                rowCount--;
            }
        }
        List<String> instIds = getDnInstId();
        sql = String.format(MOVE_PARTITIONS, tableGroup1, instIds.get(0));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(MERGE_GROUPS, tableGroup2, tableGroup1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(DROP_TABLE_GROUP, tableGroup2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tableGroup3 = "tg" + getTableGroupId("t2");
        sql = String.format(MERGE_GROUPS, tableGroup3, tableGroup1);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "ERR_JOIN_GROUP_NOT_MATCH");
        for (int i = 0; i < tableCount; i++) {
            sql = String.format(DROP_TABLE, i);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
        sql = String.format(DROP_TABLE_GROUP, tableGroup1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(DROP_JOIN_GROUP, joinGroupName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testAlterTableGroupAddTables() {
        String joinGroupName = "jg2" + RandomUtils.getStringBetween(1, 5);
        int tableCount = 10;
        String sql = String.format(CREATE_JOIN_GROUP, joinGroupName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tableGroup = "mytg4" + RandomUtils.getStringBetween(1, 5);
        sql = String.format(CREATE_TABLE_GROUP, tableGroup);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Random random = new Random();
        for (int i = 0; i < tableCount; i++) {
            sql = String.format(DROP_TABLE, i);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            sql = String.format(CREATE_TABLE, i, Math.max(1, i % 4), "");
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
        List<String> tables = new ArrayList<>();
        for (int i = 0; i < tableCount - 1; i = i + 2) {
            String tableName = "t" + i;
            tables.add(tableName);
        }
        sql = String.format(ALTER_TABLE_GROUP_ADD_TABLES, tableGroup, String.join(",", tables));
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "the partition definition of the table");

        sql = String.format(ALTER_TABLE_GROUP_ADD_TABLES_FORCE, tableGroup, String.join(",", tables));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        tables.clear();
        for (int i = 1; i < tableCount - 1; i = i + 2) {
            String tableName = "t" + i;
            tables.add(tableName);
        }
        sql = String.format(ALTER_JOIN_GROUP_ADD_TABLES, joinGroupName, String.join(",", tables));
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        for (int i = 1; i < tableCount - 1; i = i + 2) {
            String tableName = "t" + i;
            sql = String.format(ALTER_TABLE_GROUP_ADD_TABLES_FORCE, tableGroup, tableName);
            JdbcUtil.executeUpdateFailed(tddlConnection, sql, "ERR_JOIN_GROUP_NOT_MATCH");
        }

        sql = String.format(CREATE_TABLE, tableCount, 4, "");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String tableName = "t" + tableCount;
        sql = String.format(ALTER_TABLE_GROUP_ADD_TABLES, tableGroup, tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not match");
        sql = String.format(ALTER_TABLE_GROUP_ADD_TABLES_FORCE, tableGroup, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        for (int i = 0; i <= tableCount; i++) {
            sql = String.format(DROP_TABLE, i);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
        sql = String.format(DROP_TABLE_GROUP, tableGroup);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(DROP_JOIN_GROUP, joinGroupName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public List<String> getDnInstId() {
        String sql = "show storage";
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        List<String> instIds = new ArrayList<>();
        try {
            while (rs.next()) {
                if ("master".equalsIgnoreCase(rs.getString("inst_kind"))) {
                    instIds.add(rs.getString("STORAGE_INST_ID"));
                }
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
        return instIds;
    }
}
