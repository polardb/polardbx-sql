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
import java.sql.SQLException;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class JoinGroupDdlTest extends PartitionTestBase {

    private static String CREATE_JOIN_GTOUP = "create joingroup %s";
    private static String CREATE_TABLE_PATTERN1 = "create table %s(a int, b int) partition by hash(a) partitions 3";
    private static String CREATE_INDEX_PATTERN1 = "create global index %s on %s(b) partition by hash(b) partitions 3";
    private static String ADD_INDEX_PATTERN1 =
        "alter table %s add global index %s (b) partition by hash(b) partitions 3";

    @Test
    public void ddlTest() {
        String dropTable = "drop table if exists %s";
        String tb1 = "tb1" + RandomUtils.getStringBetween(1, 5);
        String tb2 = "tb2" + RandomUtils.getStringBetween(1, 5);
        String joinGroupName1 = "myJoinGroup10";
        String sql = String.format(dropTable, tb1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN1, tb1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(dropTable, tb2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE_PATTERN1, tb2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb1).longValue(),
            getTableGroupId(tb2).longValue());

        String tb3 = "tb3" + RandomUtils.getStringBetween(1, 5);
        sql = String.format(dropTable, tb3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(CREATE_TABLE_PATTERN1, tb3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals("tableGroup should be the same", getTableGroupId(tb1).longValue(),
            getTableGroupId(tb3).longValue());

        sql = String.format(CREATE_JOIN_GTOUP, joinGroupName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter joingroup %s add tables %s", joinGroupName1, tb3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tb1).longValue() != getTableGroupId(tb3).longValue());

        String indexName1 = "g1" + RandomUtils.getStringBetween(1, 5);
        sql = String.format(CREATE_INDEX_PATTERN1, indexName1, tb3);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals("tableGroup should be the same",
            getTableGroupId(tb3).longValue(), getTableGroupId(indexName1, true).longValue());

        String indexName2 = "g2" + RandomUtils.getStringBetween(1, 5);
        sql = String.format(ADD_INDEX_PATTERN1, tb3, indexName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertEquals("tableGroup should be the same",
            getTableGroupId(tb3).longValue(), getTableGroupId(indexName2, true).longValue());

    }

    @Test
    public void setTableGroup() {
        String createTableGroup = "create tablegroup %s";
        String tableGroupName1 = "myTg1" + RandomUtils.getStringBetween(1, 5);
        String joinGroupName1 = "myJoinGroup1" + RandomUtils.getStringBetween(1, 5);
        String createJoinGroup = "create joingroup %s";
        String sql = String.format(createTableGroup, tableGroupName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(createJoinGroup, joinGroupName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String CreateTablePattern1 = "create table %s(a int, b int) partition by hash(a) partitions 5 %s";
        String tbName1 = "myTb1" + RandomUtils.getStringBetween(1, 5);
        sql = String.format(CreateTablePattern1, tbName1, "joingroup=" + joinGroupName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        CreateTablePattern1 = "create table %s(a int, b int) partition by hash(a) partitions 5 %s";
        String tbName2 = "myTb2" + RandomUtils.getStringBetween(1, 5);
        sql = String.format(CreateTablePattern1, tbName2, "tablegroup=" + tableGroupName1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Assert.assertTrue("tableGroup should be different",
            getTableGroupId(tbName1).longValue() != getTableGroupId(tbName2).longValue());

        sql = "alter table " + tbName1 + " set tablegroup=" + tableGroupName1 + " force";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "ERR_JOIN_GROUP_NOT_MATCH");

        sql = "select count(1) from information_schema.join_group where JOIN_GROUP_NAME='" + joinGroupName1 + "'";
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        try {
            if (rs.next()) {
                Assert.assertTrue(rs.getInt(1) ==  1);
            }
        } catch (SQLException ex) {
            log.error(ex);
            Assert.fail(ex.getMessage());
        }

        sql = "alter joingroup " + joinGroupName1 + " add tables " + tbName1 + ", " + tbName2;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        CreateTablePattern1 = "create table %s(a int, b int) partition by hash(a) partitions 5 %s";
        String tbName3 = "myTb3";
        sql = String.format(CreateTablePattern1, tbName3, "");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter joingroup " + joinGroupName1 + " add tables " + tbName3;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table " + tbName3 + " set tablegroup=" + tableGroupName1 + " force";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select count(1) from information_schema.join_group where JOIN_GROUP_NAME='" + joinGroupName1 + "'";
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        try {
            if (rs.next()) {
                Assert.assertTrue(rs.getInt(1) == 3);
            }
        } catch (SQLException ex) {
            log.error(ex);
            Assert.fail(ex.getMessage());
        }

    }
}
