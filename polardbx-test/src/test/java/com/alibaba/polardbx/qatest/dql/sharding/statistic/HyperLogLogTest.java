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

package com.alibaba.polardbx.qatest.dql.sharding.statistic;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlOrTddl;
import static com.alibaba.polardbx.qatest.validator.PrepareData.tableDataPrepareRD;

/**
 * @author fangwu
 */

public class HyperLogLogTest extends CrudBasedLockTestCase {

    public HyperLogLogTest(String table1) {
        baseOneTableName = table1;
    }

    @Parameterized.Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableName.updateBaseOneTableForCollationTest());
    }

    @Test
    public void testShowTables() throws SQLException {
        int[] testScope = {100, 1000, 5000, 10000};
        StringBuilder stringBuilder = new StringBuilder();
        for (int scope : testScope) {
            stringBuilder.append("scope:" + scope).append("\n");
            // init random data
            tableDataPrepareRD(baseOneTableName, scope,
                TableColumnGenerator.getAllTypeColumPkAndIntegerNotNull(), PK_COLUMN_NAME,
                tddlConnection, columnDataGenerator);
            String sql = "analyze table " + baseOneTableName;
            executeOnMysqlOrTddl(tddlConnection, sql, null);

            // look up statistics
            sql = "select * from VIRTUAL_STATISTIC";
            Statement stmt = tddlConnection.createStatement();
            ResultSet rs = null;
            Map<String, Long> cardinalityMap = Maps.newHashMap();

            try {
                rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    if (!rs.getBoolean("NDV_SOURCE")) {
                        return;
                    }
                    if (rs.getString("table_name").equals(baseOneTableName)) {
                        String column = rs.getString("column_name");
                        cardinalityMap.put(column, rs.getLong("cardinality"));
                    }
                }
            } finally {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
            }

            // check statistic
            for (String column : cardinalityMap.keySet()) {
                sql = "select count(distinct " + column + ") from " + baseOneTableName;
                stmt = tddlConnection.createStatement();
                ResultSet rsCardinality = stmt.executeQuery(sql);
                rsCardinality.next();
                long real = rsCardinality.getLong(1);
                long estimate = cardinalityMap.get(column);
                String s = baseOneTableName + ":" + column + ":" + real + ":" + estimate;
                stringBuilder.append(s).append("\n");
//                System.out.println(s);
                Assert.assertTrue(s, Math.abs(real - estimate) < 100 || Double.valueOf(estimate) / real > 0.9);
            }
            stringBuilder.append("\n");
        }
//        System.out.println(stringBuilder.toString());
    }

    @Test
    public void supportHllControlWithParam() throws SQLException {
        String sql = "/*TDDL:ENABLE_HLL=false*/ analyze table " + baseOneTableName;
        Statement stmt = tddlConnection.createStatement();
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                if (rs.getString("MSG_TYPE").equalsIgnoreCase("use hll")) {
                    Assert.assertTrue("false".equalsIgnoreCase(rs.getString("MSG_TEXT")));
                    return;
                }
            }
            Assert.fail();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }
}
