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

/**
 * @version 1.0
 */
public class XPlanGetTest extends ReadBaseTestCase {

    private final String TABLE_NAME = "XPlan_get";

    private static final String TABLE_TEMPLATE = "create table {0} (\n"
        + "    pk bigint not null auto_increment,\n"
        + "    x int not null,\n"
        + "    y int default null,\n"
        + "    z int default null,\n"
        + "    key i_xy(x,y),\n"
        + "    key i_xz(x,z),\n"
        + "    primary key(pk)\n"
        + ") dbpartition by hash(pk) tbpartition by hash(x) tbpartitions 2";

    @Before
    public void initTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat
                .format(TABLE_TEMPLATE,
                    quoteSpecialName(TABLE_NAME)));
    }

    @After
    public void cleanup() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));
    }

    private void initDataXY() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "delete from " + quoteSpecialName(TABLE_NAME) + " where 1=1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + quoteSpecialName(TABLE_NAME)
                + " (x,y,z) values (1,1,1),(1,2,1),(1,3,1),(1,4,1),(1,5,1),(1,6,1),(1,7,1),(1,8,1),(1,9,1)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "clear plancache");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "analyze table " + quoteSpecialName(TABLE_NAME));
    }

    private void initDataXZ() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "delete from " + quoteSpecialName(TABLE_NAME) + " where 1=1");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + quoteSpecialName(TABLE_NAME)
                + " (x,z,y) values (1,1,1),(1,2,1),(1,3,1),(1,4,1),(1,5,1),(1,6,1),(1,7,1),(1,8,1),(1,9,1)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "clear plancache");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "analyze table " + quoteSpecialName(TABLE_NAME));
    }

    @Test
    public void testGetIndexSelection() {
        initDataXY();
        Assert.assertTrue(JdbcUtil.resultsStr(JdbcUtil.executeQuery(
                "explain /*+TDDL: cmd_extra(EXPLAIN_X_PLAN=true)*/ select * from " + quoteSpecialName(TABLE_NAME)
                    + " where x=1 and y=1 and z=1", tddlConnection))
            .contains("\"index_info\": {\"name\": {\"type\": \"V_STRING\",\"v_string\": {\"value\": \"i_xy\""));

        initDataXZ();
        Assert.assertTrue(JdbcUtil.resultsStr(JdbcUtil.executeQuery(
                "explain /*+TDDL: cmd_extra(EXPLAIN_X_PLAN=true)*/ select * from " + quoteSpecialName(TABLE_NAME)
                    + " where x=1 and y=1 and z=1", tddlConnection))
            .contains("\"index_info\": {\"name\": {\"type\": \"V_STRING\",\"v_string\": {\"value\": \"i_xz\""));
    }

}
