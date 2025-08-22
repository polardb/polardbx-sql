/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import java.sql.ResultSet;
import java.text.MessageFormat;

import static com.alibaba.polardbx.qatest.BaseSequenceTestCase.quoteSpecialName;

public class XPlanGbkTest extends ReadBaseTestCase {
    private final String TABLE_NAME = "XPlan_charset";

    private static final String TABLE_TEMPLATE = "create table {0} (\n"
        + "    pk bigint not null auto_increment,\n"
        + "    `c1` varchar(255) CHARSET utf8mb4,\n"
        + "    `c2` varchar(255) CHARSET utf8mb3,\n"
        + "    `c3` varchar(255) CHARSET gbk,\n"
        + "    key ic1(c1),\n"
        + "    key ic2(c2),\n"
        + "    key ic3(c3),\n"
        + "    primary key(pk)\n"
        + ") single";

    @Before
    public void initTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat
                .format(TABLE_TEMPLATE,
                    quoteSpecialName(TABLE_NAME)));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "insert into " + quoteSpecialName(TABLE_NAME)
                + " values (1, x'E4BDA0E5A5BD', x'E4BDA0E5A5BD', x'C4E3BAC3')");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "analyze table " + quoteSpecialName(TABLE_NAME));
    }

    @After
    public void cleanup() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));
    }

    private void traceOneRow(String sql, boolean goXplan) {
        ResultSet rs = JdbcUtil.executeQuery("trace " + sql, tddlConnection);
        final int sz = JdbcUtil.resultsSize(rs);
        Assert.assertEquals(1, sz);
        rs = JdbcUtil.executeQuery("show trace", tddlConnection);
        final String trace = JdbcUtil.getStringResult(rs, false).get(0).get(11);
        Assert.assertEquals("should go xplan", goXplan, trace.contains("plan_digest"));
    }

    @Test
    public void test() {
        // go pk and filter with c1-c3
        traceOneRow("select * from " + quoteSpecialName(TABLE_NAME) + " where pk=1", true);
        traceOneRow("select * from " + quoteSpecialName(TABLE_NAME) + " where pk=1 and c1='你好'", true);
        traceOneRow("select * from " + quoteSpecialName(TABLE_NAME) + " where pk=1 and c2='你好'", true);
        traceOneRow("select * from " + quoteSpecialName(TABLE_NAME) + " where pk=1 and c3='你好'", true);

        // allow str with utf8, but not gbk
        traceOneRow("select * from " + quoteSpecialName(TABLE_NAME) + " where c1='你好'", true);
        traceOneRow("select * from " + quoteSpecialName(TABLE_NAME) + " where c2='你好'", true);
        traceOneRow("select * from " + quoteSpecialName(TABLE_NAME) + " where c3='你好'", false);
    }
}
