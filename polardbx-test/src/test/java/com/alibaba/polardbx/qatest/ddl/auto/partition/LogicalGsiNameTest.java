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
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class LogicalGsiNameTest extends PartitionTestBase {
    final static String CREATE_TABLE_GROUP = "create tablegroup %s";

    final static String CREATE_TABLE = "create table %s(a int) partition by hash(a) partitions %d %s";
    final static String DROP_TABLE = "drop table if exists %s";
    final static String ALTER_TABLE_SET_TABLEGROUP = "alter table %s set tablegroup=%s";
    final static String SHOW_FULL_TABLE_GROUP =
        "show full tablegroup where table_schema='%s' and table_group_name='%s' ";
    final static String CREATE_GSI = "create global index %s on %s(a) partition by hash(a) partitions %d %s";
    final static String LOGICAL_GSI = "%s.%s";
    final static String ALTER_TABLEGROUP_ADD_TABLE = "alter tablegroup %s add tables %s";

    @Test
    public void showfullTableGroupTest() {
        String tableGroup1 = "pxctg1";
        String sql = String.format(CREATE_TABLE_GROUP, tableGroup1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tbName = "tb1234567";
        sql = String.format(DROP_TABLE, tbName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE, tbName, 3, "tablegroup=" + tableGroup1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String gsiName = "g1";
        sql = String.format(CREATE_GSI, gsiName, tbName, 3, "tablegroup=" + tableGroup1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(SHOW_FULL_TABLE_GROUP, tddlDatabase1, tableGroup1);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        String tables = StringUtils.EMPTY;
        try {
            if (rs.next()) {
                tables = rs.getString("tables");
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
        String logicalGsiName = String.format(LOGICAL_GSI, tbName, gsiName);
        assertWithMessage("index of logicGsiName is " + tables.indexOf(logicalGsiName)).that(
            tables.indexOf(logicalGsiName)).isGreaterThan(-1);
    }

    @Test
    public void alterTableSetTableGroupTest() {
        String tableGroup2 = "pxctg2";
        String sql = String.format(CREATE_TABLE_GROUP, tableGroup2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String tbName = "tb1234568";
        sql = String.format(DROP_TABLE, tbName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE, tbName, 3, "");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String gsiName = "g2";
        sql = String.format(CREATE_GSI, gsiName, tbName, 3, "");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String logicalGsiName = String.format(LOGICAL_GSI, tbName, gsiName);

        sql = String.format(ALTER_TABLE_SET_TABLEGROUP, logicalGsiName, tableGroup2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(SHOW_FULL_TABLE_GROUP, tddlDatabase1, tableGroup2);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        String tables = StringUtils.EMPTY;
        try {
            if (rs.next()) {
                tables = rs.getString("tables");
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
        Assert.assertTrue(tables.indexOf(logicalGsiName) > -1);

    }

    @Test
    public void altetTableGroupAddTableTest() {
        String tableGroup1 = "pxctg3";
        String sql = String.format(CREATE_TABLE_GROUP, tableGroup1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String tbName = "tb1234569";
        sql = String.format(DROP_TABLE, tbName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(CREATE_TABLE, tbName, 3, "");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String gsiName = "g1";
        sql = String.format(CREATE_GSI, gsiName, tbName, 3, "");
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String indexName = String.format("%s.%s", tbName, gsiName);
        sql = String.format(ALTER_TABLEGROUP_ADD_TABLE, tableGroup1, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(SHOW_FULL_TABLE_GROUP, tddlDatabase1, tableGroup1);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        String tables = StringUtils.EMPTY;
        try {
            if (rs.next()) {
                tables = rs.getString("tables");
            }
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
        Assert.assertTrue(tables.indexOf(indexName) > -1);
    }
}
