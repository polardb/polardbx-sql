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

package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试show带where条件： 1.filter里的数值类型是否解析正确 2.filter里无法识别的列名是否能报错 3.function是否支持常数
 * 4.常量表达式优化后的结果是否有效
 *
 * @author minggong 2017-09-05 21:49
 */
public class ShowWithWhereTest extends ReadBaseTestCase {

    @Test
    public void testShowProcesslist() throws SQLException {
        String sql = "show processlist where time < ? and time > ?";
        long[][] testValues = {{100, 9}};

        List<Object> param = new ArrayList<Object>();
        for (long[] values : testValues) {
            param.clear();
            for (long value : values) {
                param.add(value);
            }

            PreparedStatement tddlPs = null;
            ResultSet rs = null;
            try {
                tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
                rs = JdbcUtil.executeQuery(sql, tddlPs);
                while (rs.next()) {
                    Long time = rs.getLong("TIME");
                    Assert.assertTrue(time < values[0] && time > values[1]);
                }
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            } finally {
                JdbcUtil.close(rs);
                JdbcUtil.close(tddlPs);
            }
        }
    }

    @Test(expected = SQLException.class)
    public void testShowRuleStatus() throws SQLException {
        String sql = "show rule status where version * ? + ? > ?";
        int[][] testValues = {{2, 5, 1000000}};

        List<Object> param = new ArrayList<Object>();
        for (int[] values : testValues) {
            param.clear();
            for (int value : values) {
                param.add(value);
            }

            PreparedStatement tddlPs = null;
            ResultSet rs = null;
            try {
                tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
                rs = tddlPs.executeQuery();
                while (rs.next()) {
                    Integer version = rs.getInt("VERSION");
                    Assert.assertTrue(version * values[0] + values[1] > values[2]);
                }
            } finally {
                JdbcUtil.close(rs);
                JdbcUtil.close(tddlPs);
            }
        }
    }

    @Test
    public void testShowProcedureStatus() throws SQLException {
        String sql = "show procedure status where created > ?";
        String[] testValues = {"2017-9-01 00:00:00"};

        List<Object> param = new ArrayList<Object>();
        for (String value : testValues) {
            param.clear();
            Timestamp timeValue = Timestamp.valueOf(value);
            param.add(timeValue);

            PreparedStatement tddlPs = null;
            ResultSet rs = null;
            try {
                tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
                rs = JdbcUtil.executeQuery(sql, tddlPs);
                while (rs.next()) {
                    Timestamp time = rs.getTimestamp("CREATED");
                    Assert.assertTrue(time.compareTo(timeValue) > 0);
                }
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            } finally {
                JdbcUtil.close(rs);
                JdbcUtil.close(tddlPs);
            }
        }
    }

    @Test
    public void testShowBroadcasts() throws SQLException {

        String sql = "show broadcasts where power(id, 2) < power(?, 2)";
        int[] testValues = {5};

        List<Object> param = new ArrayList<Object>();
        for (int value : testValues) {
            param.clear();
            param.add(value);

            PreparedStatement tddlPs = null;
            ResultSet rs = null;
            try {
                tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
                rs = JdbcUtil.executeQuery(sql, tddlPs);
                while (rs.next()) {
                    Integer id = rs.getInt("ID");
                    Assert.assertTrue(id < value);
                }
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            } finally {
                JdbcUtil.close(rs);
                JdbcUtil.close(tddlPs);
            }
        }
    }

    @Test
    public void testLiteralExpr() throws SQLException {
        String sql = "show processlist where 1 = 2";

        PreparedStatement tddlPs = null;
        ResultSet rs = null;
        try {
            tddlPs = JdbcUtil.preparedStatementSet(sql, null, tddlConnection);
            rs = JdbcUtil.executeQuery(sql, tddlPs);
            if (rs.next()) {
                Assert.fail("should return nothing");
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(tddlPs);
        }
    }
}
