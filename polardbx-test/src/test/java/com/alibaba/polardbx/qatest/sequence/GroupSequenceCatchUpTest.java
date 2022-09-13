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

package com.alibaba.polardbx.qatest.sequence;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by chensr on 2017-10-31 测试 Group Sequence catches up with explicit
 * insert value.
 */

public class GroupSequenceCatchUpTest extends BaseSequenceTestCase {

    private String tableName;

    public GroupSequenceCatchUpTest(String schema) {
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
        this.tableName = schemaPrefix + randomTableName("tempForGroupSeqCatchUp7", 4);
    }

    @Parameterized.Parameters(name = "{index}:schema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {{""}, {PropertiesUtil.polardbXShardingDBName2()}});
    }

    @Before
    public void init() {
        // create a sharding table with group sequence
        String sql = String
            .format(
                "create table if not exists %s(c1 int auto_increment by group, c2 int, primary key(c1)) dbpartition by hash(c1)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @After
    public void destroy() {
        // drop the temporary table
        String sql = String.format("drop table if exists %s", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Test
    public void testGroupSeqCatchUp() throws Exception {
        long groupSeqStep = 100000;
        long checkInterval = 5; // 5 seconds for test only (diamond config changed already)

        String insertWithSeq = String.format("insert into %s(c2) values(1)", tableName);
        String insertWithVal = String.format("insert into %s values(%s, 1)", tableName, "%s");

        // Insert and generate the first group sequence value
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertWithSeq);
        long firstSeqVal = getLastInsertVal();

        Assert.assertTrue(firstSeqVal == groupSeqStep + 1);

        // ---------------------------------------------------------------------------
        // Scenario 1: Insert an explicit value that is greater than current value and
        // less than maximum value of current sequence range
        // ---------------------------------------------------------------------------
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertWithVal, firstSeqVal + groupSeqStep / 2));
        long firstExpVal = getLastInsertVal();

        Assert.assertTrue(firstExpVal == groupSeqStep + 1);

        // Sleep for some time to wait for timed task to take effect
        Thread.sleep(checkInterval * 1000);

        // Insert again to get new sequence value caught up with
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertWithSeq);
        long secondSeqVal = getLastInsertVal();

        Assert.assertTrue(secondSeqVal == firstSeqVal + groupSeqStep / 2 + 1);

        // ---------------------------------------------------------------------------
        // Scenario 2: Insert an explicit value that is greater than maximum value of
        // current sequence range
        // ---------------------------------------------------------------------------
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertWithVal, firstSeqVal + groupSeqStep * 1.5));
        long secondExpVal = getLastInsertVal();

        Assert.assertTrue(secondExpVal == firstSeqVal + groupSeqStep / 2 + 1);

        // Sleep for some time to wait for timed task to take effect
        Thread.sleep(checkInterval * 1000);

        // Insert again to get new sequence value caught up with
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertWithSeq);
        long thirdSeqVal = getLastInsertVal();

        Assert.assertTrue(thirdSeqVal == firstSeqVal + groupSeqStep * 2);

        // ---------------------------------------------------------------------------
        // Scenario 3: Insert an explicit value that is less than current value of
        // current sequence range
        // ---------------------------------------------------------------------------
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertWithVal, firstSeqVal + groupSeqStep * 1.8));
        long thirdExpVal = getLastInsertVal();

        Assert.assertTrue(thirdExpVal == firstSeqVal + groupSeqStep * 2);

        // Sleep for some time to wait for timed task to take effect
        Thread.sleep(checkInterval * 1000);

        // Insert again to get new sequence value caught up with
        JdbcUtil.executeUpdateSuccess(tddlConnection, insertWithSeq);
        long fouthSeqVal = getLastInsertVal();

        Assert.assertTrue(fouthSeqVal == firstSeqVal + groupSeqStep * 2 + 1);
    }

    private long getLastInsertVal() {
        String selectLastInsertId = "select last_insert_id()";
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, selectLastInsertId);
            if (rs.next()) {
                return rs.getLong(1);
            } else {
                Assert.fail("Failed to get last_insert_id");
                return 0L;
            }
        } catch (Exception e) {
            Assert.fail("Failed to executeSuccess \"select last_insert_id()\": " + e.getMessage());
            return 0L;
        } finally {
            try {
                rs.close();
            } catch (SQLException e) {
            }
        }
    }

}
