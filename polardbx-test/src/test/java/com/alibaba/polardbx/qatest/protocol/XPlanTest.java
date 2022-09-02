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
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.BaseSequenceTestCase.quoteSpecialName;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @version 1.0
 */

@NotThreadSafe
public class XPlanTest extends ReadBaseTestCase {

    private static final String PK_TABLE_NAME = "XPlan_pk";
    private static final String NO_PK_TABLE_NAME = "XPlan_no_pk";
    private static final String COMPOSITE_PK_TABLE_NAME = "XPlan_composite_pk";
    private static final String MULTI_KEY_TABLE_NAME = "XPlan_multi_key";

    private static final String[] PARTITIONS_TEMPLATE = {
        "",
        " broadcast",
        " dbpartition by hash(pk)",
        " dbpartition by hash(pk) tbpartition by hash(x) tbpartitions 2"
    };

    private static final String[] PARTITIONS_TEMPLATE_NEW_PART = {
        " broadcast",
        " partition by key(pk) partitions 3"
    };

    private static final String PK_TABLE_TEMPLATE = "create table {0} (\n"
        + "    pk bigint not null,\n"
        + "    x int not null,\n"
        + "    y int default null,\n"
        + "    z int default null,\n"
        + "    primary key(pk)\n"
        + ")";
    private static final String NO_PK_TABLE_TEMPLATE = "create table {0} (\n"
        + "    pk bigint not null,\n"
        + "    x int not null,\n"
        + "    y int default null,"
        + "    z int default null\n"
        + ")";
    private static final String COMPOSITE_PK_TABLE_TEMPLATE = "create table {0} (\n"
        + "    pk bigint not null,\n"
        + "    x int not null,\n"
        + "    y int default null,\n"
        + "    z int default null,\n"
        + "    primary key(pk,x)\n"
        + ")";
    private static final String MULTI_KEY_TABLE_TEMPLATE = "create table {0} (\n"
        + "    pk bigint not null,\n"
        + "    x int not null,\n"
        + "    y int default null,\n"
        + "    z int default null,\n"
        + "    primary key(pk),\n"
        + "    key kpk(pk,x,y),"
        + "    key kx(x),\n"
        + "    key ky(y)\n"
        + ")";

    private static final String INSERT_TEMPLATE = "insert into {0} values "
        + "(1,101,1001,10001),(2,102,null,10002),(3,103,1003,null),(4,104,null,null),(5,105,1005,10005);";

    private final String partition;

    public XPlanTest(String partition) {
        this.partition = partition;
    }

    @Parameterized.Parameters(name = "{index}:pk={0}")
    public static List<String[]> prepareDate() {
        return Arrays.stream(PARTITIONS_TEMPLATE)
            .map(s -> new String[] {s})
            .collect(Collectors.toList());
    }

    @Before
    public void initData() {
        if (usingNewPartDb()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(PK_TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(NO_PK_TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(COMPOSITE_PK_TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(MULTI_KEY_TABLE_NAME));

        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "drop table if exists " + quoteSpecialName(PK_TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "drop table if exists " + quoteSpecialName(NO_PK_TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "drop table if exists " + quoteSpecialName(COMPOSITE_PK_TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            "drop table if exists " + quoteSpecialName(MULTI_KEY_TABLE_NAME));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(PK_TABLE_TEMPLATE + partition, quoteSpecialName(PK_TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(NO_PK_TABLE_TEMPLATE + partition, quoteSpecialName(NO_PK_TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(COMPOSITE_PK_TABLE_TEMPLATE + partition, quoteSpecialName(COMPOSITE_PK_TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(MULTI_KEY_TABLE_TEMPLATE + partition, quoteSpecialName(MULTI_KEY_TABLE_NAME)));

        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            MessageFormat.format(PK_TABLE_TEMPLATE, quoteSpecialName(PK_TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            MessageFormat.format(NO_PK_TABLE_TEMPLATE, quoteSpecialName(NO_PK_TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            MessageFormat.format(COMPOSITE_PK_TABLE_TEMPLATE, quoteSpecialName(COMPOSITE_PK_TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            MessageFormat.format(MULTI_KEY_TABLE_TEMPLATE, quoteSpecialName(MULTI_KEY_TABLE_NAME)));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(INSERT_TEMPLATE, quoteSpecialName(PK_TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(INSERT_TEMPLATE, quoteSpecialName(NO_PK_TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(INSERT_TEMPLATE, quoteSpecialName(COMPOSITE_PK_TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat.format(INSERT_TEMPLATE, quoteSpecialName(MULTI_KEY_TABLE_NAME)));

        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            MessageFormat.format(INSERT_TEMPLATE, quoteSpecialName(PK_TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            MessageFormat.format(INSERT_TEMPLATE, quoteSpecialName(NO_PK_TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            MessageFormat.format(INSERT_TEMPLATE, quoteSpecialName(COMPOSITE_PK_TABLE_NAME)));
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
            MessageFormat.format(INSERT_TEMPLATE, quoteSpecialName(MULTI_KEY_TABLE_NAME)));
    }

    private final static String[] simpleTestTemplates = {
        "select * from {0} where pk = null",
        "select * from {0} where pk <=> null",
        "select * from {0} where pk is null",
        "select * from {0} where pk = 1",
        "select * from {0} where pk <=> 1",

        "select * from {0} where x = null",
        "select * from {0} where x <=> null",
        "select * from {0} where x is null",
        "select * from {0} where x = 101",
        "select * from {0} where x <=> 101",

        "select * from {0} where y = null",
        "select * from {0} where y <=> null",
        "select * from {0} where y is null",
        "select * from {0} where y = 1001",
        "select * from {0} where y <=> 1001",

        "select * from {0} where z = null",
        "select * from {0} where z <=> null",
        "select * from {0} where z is null",
        "select * from {0} where z = 1001",
        "select * from {0} where z <=> 1001",
    };

    @Test
    public void testSingleSelect() {
        if (usingNewPartDb()) {
            return;
        }
        for (String sql : simpleTestTemplates) {
            selectContentSameAssert(MessageFormat.format(sql, quoteSpecialName(PK_TABLE_NAME)), null,
                mysqlConnection, tddlConnection, true);
            selectContentSameAssert(MessageFormat.format(sql, quoteSpecialName(NO_PK_TABLE_NAME)), null,
                mysqlConnection, tddlConnection, true);
            selectContentSameAssert(MessageFormat.format(sql, quoteSpecialName(COMPOSITE_PK_TABLE_NAME)), null,
                mysqlConnection, tddlConnection, true);
            selectContentSameAssert(MessageFormat.format(sql, quoteSpecialName(MULTI_KEY_TABLE_NAME)), null,
                mysqlConnection, tddlConnection, true);
        }
    }

    private void enableXPlanTableScan() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL: node('__META_DB__')*/update inst_config set param_val=\"true\" where param_key=\"CONN_POOL_XPROTO_XPLAN_TABLE_SCAN\"");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL: node('__META_DB__')*/update config_listener set op_version=op_version+1 where data_id like \"polardbx.inst.config.%\"");
        Thread.sleep(3000);
    }

    private void disableXPlanTableScan() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL: node('__META_DB__')*/update inst_config set param_val=\"false\" where param_key=\"CONN_POOL_XPROTO_XPLAN_TABLE_SCAN\"");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "/*+TDDL: node('__META_DB__')*/update config_listener set op_version=op_version+1 where data_id like \"polardbx.inst.config.%\"");
        Thread.sleep(3000);
    }

    private void assertX(String sql, boolean useX) {
        final String exp = getExplainResult(tddlConnection, "/*+TDDL: cmd_extra(EXPLAIN_X_PLAN=true)*/" + sql);
        final boolean actualX = exp.contains("XPlan=");
        //System.out.println(exp);
        Assert.assertTrue("Bad XPlan. " + exp, (useX && actualX) || (!useX && !actualX));
    }

    @Test
    public void testExplain() throws Exception {
        if (usingNewPartDb()) {
            return;
        }
        enableXPlanTableScan();

        try {
            final boolean with_partition = partition.contains("dbpartition");

            for (String sql : simpleTestTemplates) {
                assertX(MessageFormat.format(sql, quoteSpecialName(PK_TABLE_NAME)), true);
                assertX(MessageFormat.format(sql, quoteSpecialName(NO_PK_TABLE_NAME)), true);
                assertX(MessageFormat.format(sql, quoteSpecialName(COMPOSITE_PK_TABLE_NAME)), true);
                assertX(MessageFormat.format(sql, quoteSpecialName(MULTI_KEY_TABLE_NAME)), true);
            }

            // Again for cache.
            for (String sql : simpleTestTemplates) {
                assertX(MessageFormat.format(sql, quoteSpecialName(PK_TABLE_NAME)), true);
                assertX(MessageFormat.format(sql, quoteSpecialName(NO_PK_TABLE_NAME)), true);
                assertX(MessageFormat.format(sql, quoteSpecialName(COMPOSITE_PK_TABLE_NAME)), true);
                assertX(MessageFormat.format(sql, quoteSpecialName(MULTI_KEY_TABLE_NAME)), true);
            }
        } finally {
            disableXPlanTableScan();
        }
    }

}
