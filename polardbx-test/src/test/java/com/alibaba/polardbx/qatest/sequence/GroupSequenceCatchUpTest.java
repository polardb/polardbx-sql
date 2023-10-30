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
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.BaseSequenceTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by chensr on 2017-10-31 测试 Group Sequence catches up with explicit
 * insert value.
 */
public class GroupSequenceCatchUpTest extends BaseSequenceTestCase {

    private static final Log log = LogFactory.getLog(GroupSequenceCatchUpTest.class);

    private String simpleTableName;
    private String tableName;
    private Connection conn;
    private Connection conn2;

    public GroupSequenceCatchUpTest(String schema) {
        this.schema = schema;
        this.schemaPrefix = StringUtils.isBlank(schema) ? "" : schema + ".";
    }

    @Parameterized.Parameters(name = "{index}:schema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {{""}, {PropertiesUtil.polardbXShardingDBName2()}});
    }

    @Before
    public void init() {
        // create a sharding table with group sequence
        this.simpleTableName = randomTableName("tempForGroupSeqCatchUp7", 4);
        this.tableName = schemaPrefix + simpleTableName;
        this.conn = getPolardbxConnection();
        this.conn2 = getPolardbxConnection2();

        String sql = String
            .format(
                "create table if not exists %s(c1 int auto_increment by group, c2 int, primary key(c1)) dbpartition by hash(c1)",
                tableName);
        JdbcUtil.executeUpdateSuccess(conn, sql);
    }

    @After
    public void destroy() {
        // drop the temporary table
        String sql = String.format("drop table if exists %s", tableName);
        JdbcUtil.executeUpdateSuccess(conn, sql);

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        }

        if (conn2 != null) {
            try {
                conn2.close();
            } catch (SQLException ignored) {
            }
        }
    }

    @Test
    @Ignore
    public void testGroupSeqCatchUp() throws Exception {
        long groupSeqStep = 100000;
        long checkInterval = 70000;

        String insertWithSeq = String.format("insert into %s(c2) values(1)", tableName);
        String insertWithVal = String.format("insert into %s values(%s, 1)", tableName, "%s");

        // Insert and generate the first group sequence value
        long firstSeqValue = groupSeqStep + 1;
        JdbcUtil.executeUpdateSuccess(conn, insertWithSeq);
        inspectRange(1, firstSeqValue);

        // ---------------------------------------------------------------------------
        // Scenario 1: Insert an explicit value that is greater than current value and
        // less than maximum value of current sequence range
        // ---------------------------------------------------------------------------
        long firstExplicitValue = firstSeqValue + groupSeqStep / 2;
        JdbcUtil.executeUpdateSuccess(conn, String.format(insertWithVal, firstExplicitValue));
        inspectRange(2, firstExplicitValue);

        // Sleep for some time to wait for timed task to take effect
        Thread.sleep(checkInterval);
        inspectRange(3, firstExplicitValue);

        // Insert again to get new sequence value caught up with
        long secondSeqValue = firstExplicitValue + 1;
        JdbcUtil.executeUpdateSuccess(conn, insertWithSeq);
        inspectRange(4, secondSeqValue);

        // ---------------------------------------------------------------------------
        // Scenario 2: Insert an explicit value that is greater than maximum value of
        // current sequence range
        // ---------------------------------------------------------------------------
        long secondExplicitValue = firstExplicitValue + groupSeqStep;
        JdbcUtil.executeUpdateSuccess(conn, String.format(insertWithVal, secondExplicitValue));
        inspectRange(5, secondExplicitValue);

        // Sleep for some time to wait for timed task to take effect
        Thread.sleep(checkInterval);
        inspectRange(6, secondExplicitValue);

        // Insert again to get new sequence value caught up with
        long thirdSeqValue = groupSeqStep * 3 + 1;
        JdbcUtil.executeUpdateSuccess(conn, insertWithSeq);
        inspectRange(7, thirdSeqValue);

        // ---------------------------------------------------------------------------
        // Scenario 3: Insert an explicit value that is less than current value of
        // current sequence range
        // ---------------------------------------------------------------------------
        long thirdExplicitValue = secondExplicitValue + groupSeqStep * 3 / 10;
        JdbcUtil.executeUpdateSuccess(conn, String.format(insertWithVal, thirdExplicitValue));
        inspectRange(8, thirdExplicitValue);

        // Sleep for some time to wait for timed task to take effect
        Thread.sleep(checkInterval);
        inspectRange(9, thirdExplicitValue);

        // Insert again to get new sequence value caught up with
        long fourthSeqValue = thirdSeqValue + 1;
        JdbcUtil.executeUpdateSuccess(conn, insertWithSeq);
        inspectRange(10, fourthSeqValue);

        // Check results finally
        compareResult(new long[] {
            firstSeqValue, firstExplicitValue, secondSeqValue, secondExplicitValue, thirdExplicitValue, thirdSeqValue,
            fourthSeqValue});
    }

    private void compareResult(long[] expectedValues) {
        final String sql = "select c1 from %s order by c1";
        final List<Long> actualValues = new ArrayList<>();

        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, tableName))) {
            while (rs.next()) {
                actualValues.add(rs.getLong(1));
            }
        } catch (Exception e) {
            Assert.fail("Failed to fetch result: " + e.getMessage());
        }

        if (expectedValues.length != actualValues.size()) {
            Assert.fail("Unexpected number of values: expected - " + expectedValues.length + ", actual - "
                + actualValues.size());
        }

        for (int i = 0; i < expectedValues.length; i++) {
            if (expectedValues[i] != actualValues.get(i)) {
                printResult(expectedValues, actualValues);
                Assert.fail(
                    "Found different value: expected - " + expectedValues[i] + ", actual - " + actualValues.get(i));
            }
        }
    }

    private void printResult(long[] expectedValues, List<Long> actualValues) {
        StringBuilder buf = new StringBuilder();
        buf.append("\nExpected:\n");
        for (long expected : expectedValues) {
            buf.append(expected).append(", ");
        }
        buf.append("\nActual: \n");
        for (long actual : actualValues) {
            buf.append(actual).append(", ");
        }
        log.error(buf);
    }

    private void inspectRange(int index, long value) {
        final String sql = "inspect sequence range for AUTO_SEQ_%s";

        StringBuilder buf = new StringBuilder();
        buf.append("\n").append(index).append("------------------------\n");
        buf.append(value);
        buf.append("\n").append(index).append("------------------------\n");
        buf.append("NODE, RANGE [ MIN, MAX ]");
        buf.append("\n").append(index).append("------------------------\n");

        Connection connection = TStringUtil.isBlank(schema) ? conn : conn2;

        try (Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, simpleTableName))) {
            while (rs.next()) {
                buf.append(rs.getString(1)).append(", ");
                buf.append(rs.getString(2)).append("\n");
            }
        } catch (Exception ignored) {
        }
        buf.append(index).append("------------------------\n");
        log.error(buf);
    }

}
