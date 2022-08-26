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

package com.alibaba.polardbx.qatest.protocol;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.MessageFormat;

import static com.alibaba.polardbx.qatest.BaseSequenceTestCase.quoteSpecialName;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @version 1.0
 */
public class XPlanGetSubPartTest extends ReadBaseTestCase {
    private final String TABLE_NAME = "XPlan_get_sp";

    private static final String TABLE_TEMPLATE = "create table {0} (\n"
        + "    pk bigint not null auto_increment,\n"
        + "    x varchar(50) default null,\n"
        + "    y varchar(50) default null,\n"
        + "    z varchar(50) default null,\n"
        + "    key i_xy(x,y(1)),\n"
        + "    key i_z(z(1)),\n"
        + "    key i_zx(z(1), x),\n"
        + "    primary key(pk)\n"
        + ")";
    private static final String PARTITION_DEF = " dbpartition by hash(pk) tbpartition by hash(pk) tbpartitions 2";

    private static final String NEW_PARTITION_DEF = "partition by key(pk) partitions 3";

    @Before
    public void initTable() {
        if (usingNewPartDb()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));

        String partDef = usingNewPartDb() ? NEW_PARTITION_DEF : PARTITION_DEF;
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(TABLE_TEMPLATE + partDef, quoteSpecialName(TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            MessageFormat.format(TABLE_TEMPLATE, quoteSpecialName(TABLE_NAME)));
    }

    @After
    public void cleanup() {
        if (usingNewPartDb()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));
    }

    private void initData() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "delete from " + quoteSpecialName(TABLE_NAME) + " where 1=1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + quoteSpecialName(TABLE_NAME)
                + " (x,z,y) values ('abc','abc','abc'),('abd','abd','abc'),('abe','abd','abd'),('abf','abe','abc'),('abg','abe','abe')");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "clear plancache");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "analyze table " + quoteSpecialName(TABLE_NAME));

        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "delete from " + quoteSpecialName(TABLE_NAME) + " where 1=1");
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "insert into " + quoteSpecialName(TABLE_NAME)
                + " (x,z,y) values ('abc','abc','abc'),('abd','abd','abc'),('abe','abd','abd'),('abf','abe','abc'),('abg','abe','abe')");
    }

    @Test
    public void testSubPartIndex() {

        if (usingNewPartDb()) {
            return;
        }
        initData();

        // No extra filter. table_proj->get
        String sql = "select x,y,z from " + quoteSpecialName(TABLE_NAME) + " where x='abc'";
        String exp = JdbcUtil.resultsStr(JdbcUtil.executeQuery(
            "explain /*+TDDL: cmd_extra(EXPLAIN_X_PLAN=true)*/ " + sql, tddlConnection));
        System.out.println(exp);
        Assert.assertTrue(exp.contains(
            "XPlan=\"{\"plan\": {\"plan_type\": \"TABLE_PROJECT\",\"table_project\": {\"sub_read_plan\": {\"plan_type\": \"GET\""));
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        // y with sub part. flt->table_proj->get
        sql = "select x,y,z from " + quoteSpecialName(TABLE_NAME) + " where x='abe' and y='abd'";
        exp = JdbcUtil.resultsStr(JdbcUtil.executeQuery(
            "explain /*+TDDL: cmd_extra(EXPLAIN_X_PLAN=true)*/ " + sql, tddlConnection));
        System.out.println(exp);
        Assert.assertTrue(exp.contains(
            "XPlan=\"{\"plan\": {\"plan_type\": \"FILTER\",\"filter\": {\"sub_read_plan\": {\"plan_type\": \"TABLE_PROJECT\",\"table_project\": {\"sub_read_plan\": {\"plan_type\": \"GET\""));
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        // Single s with sub part. flt->table_proj->get
        sql = "select x,y,z from " + quoteSpecialName(TABLE_NAME) + " where z='abc'";
        exp = JdbcUtil.resultsStr(JdbcUtil.executeQuery(
            "explain /*+TDDL: cmd_extra(EXPLAIN_X_PLAN=true)*/ " + sql, tddlConnection));
        System.out.println(exp);
        Assert.assertTrue(exp.contains(
            "XPlan=\"{\"plan\": {\"plan_type\": \"FILTER\",\"filter\": {\"sub_read_plan\": {\"plan_type\": \"TABLE_PROJECT\",\"table_project\": {\"sub_read_plan\": {\"plan_type\": \"GET\""));
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        // Go zx get. flt->table_proj->get
        sql = "select x,y,z from " + quoteSpecialName(TABLE_NAME) + " where x='abg' and z='abe'";
        exp = JdbcUtil.resultsStr(JdbcUtil.executeQuery(
            "explain /*+TDDL: cmd_extra(EXPLAIN_X_PLAN=true)*/ " + sql, tddlConnection));
        System.out.println(exp);
        Assert.assertTrue(exp.contains(
            "XPlan=\"{\"plan\": {\"plan_type\": \"FILTER\",\"filter\": {\"sub_read_plan\": {\"plan_type\": \"TABLE_PROJECT\",\"table_project\": {\"sub_read_plan\": {\"plan_type\": \"GET\""));
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

}
