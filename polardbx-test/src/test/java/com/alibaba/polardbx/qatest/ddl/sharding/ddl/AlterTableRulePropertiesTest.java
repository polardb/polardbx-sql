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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author shicai.xsc 2019/3/7 上午9:45
 * @since 5.0.0.0
 */
public class AlterTableRulePropertiesTest extends DDLBaseNewDBTestCase {

    private static String TABLE_NAME = "table_rule_test";

    @Before
    public void init() {

        String dropSql = String.format("drop table if exists %s", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);

        String createSql = String.format("create table %s (id int, value int) dbpartition by hash(id)", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

    }

    @After
    public void after() {
        String dropSql = String.format("drop table if exists %s", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);
    }

    @Test
    public void alterBroadcast() throws SQLException {
        String showRuleSql = String.format("show rule from %s", TABLE_NAME);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showRuleSql);
        Assert.assertTrue(rs.next());
        long broadcast = rs.getLong("BROADCAST");
        Assert.assertEquals(broadcast, 0);

        String alterRuleSql = String.format("ALTER TABLE %s SET TBLPROPERTIES (RULE.BROADCAST = true)", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterRuleSql);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, showRuleSql);
        Assert.assertTrue(rs.next());
        broadcast = rs.getLong("BROADCAST");
        Assert.assertEquals(broadcast, 1);

        alterRuleSql = String.format("ALTER TABLE %s SET TBLPROPERTIES (RULE.BROADCAST = false)", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterRuleSql);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, showRuleSql);
        Assert.assertTrue(rs.next());
        broadcast = rs.getLong("BROADCAST");
        Assert.assertEquals(broadcast, 0);
    }

    @Test
    public void alterAllowFullTableScan() throws SQLException {
        String showRuleSql = String.format("show full rule from %s", TABLE_NAME);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, showRuleSql);
        Assert.assertTrue(rs.next());
        long allowFullTableScan = rs.getLong("ALLOW_FULL_TABLE_SCAN");
        Assert.assertEquals(allowFullTableScan, 1);

        String alterRuleSql = String.format("ALTER TABLE %s SET TBLPROPERTIES (RULE.allowFullTableScan = false)",
            TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterRuleSql);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, showRuleSql);
        Assert.assertTrue(rs.next());
        allowFullTableScan = rs.getLong("ALLOW_FULL_TABLE_SCAN");
        Assert.assertEquals(allowFullTableScan, 0);

        alterRuleSql = String.format("ALTER TABLE %s SET TBLPROPERTIES (RULE.allowFullTableScan = true)", TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterRuleSql);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, showRuleSql);
        Assert.assertTrue(rs.next());
        allowFullTableScan = rs.getLong("ALLOW_FULL_TABLE_SCAN");
        Assert.assertEquals(allowFullTableScan, 1);
    }

    @Test
    public void alterNotExistProperty() {
        String alterRuleSql = String.format("ALTER TABLE %s SET TBLPROPERTIES (RULE.not_exist_property = true)",
            TABLE_NAME);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            alterRuleSql,
            "Property RULE.not_exist_property is not allowed to change");
    }
}
