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

package com.alibaba.polardbx.qatest.dql.auto.explain;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class ExplainExecuteTest extends ReadBaseTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private static final Log log = LogFactory.getLog(ExplainExecuteTest.class);

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectOneTableMultiRuleMode());
    }

    public ExplainExecuteTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainSelectTest() {
        String sql = "explain execute select * from " + baseOneTableName;
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                rowsize++;
            }
            Assert.assertTrue(rowsize > 0);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainSelectWithPartitionFilterTest() {
        String sql = "explain execute select * from " + baseOneTableName + " where pk=1";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                String extra = rs.getString("Extra");
                Assert.assertTrue(extra != null && extra.contains("XPlan"));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }

        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery("/*+TDDL:cmd_extra(CONN_POOL_XPROTO_XPLAN=false)*/" + sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                String extra = rs.getString("Extra");
                Assert.assertTrue(extra == null || !extra.contains("XPlan"));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainUpdateTest() {
        String sql = "explain execute update " + baseOneTableName + " set varchar_test='a'";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                String extra = rs.getString("Extra");
                Assert.assertTrue(extra == null || !extra.contains("XPlan"));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Test
    public void explainJoinTest() {
        String sql = "explain execute select * from %s a join %s b on a.varchar_test = b.varchar_test and a.pk=1";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(String.format(sql, baseOneTableName, baseTwoTableName));
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                String extra = rs.getString("Extra");
                Assert.assertTrue(extra == null || !extra.contains("XPlan"));
                rowsize++;
            }
            Assert.assertTrue(rowsize > 0);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainUpdateWithPartitionFilterTest() {
        String sql = "explain execute update " + baseOneTableName + " set varchar_test='a' where pk=1";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                String extra = rs.getString("Extra");
                Assert.assertTrue(extra == null || !extra.contains("XPlan"));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainDeleteTest() {
        String sql = "explain execute delete " + baseOneTableName;
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                String extra = rs.getString("Extra");
                Assert.assertTrue(extra == null || !extra.contains("XPlan"));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainDeleteWithPartitionFilterTest() {
        String sql = "explain execute delete " + baseOneTableName + " where varchar_test='a'";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                String extra = rs.getString("Extra");
                Assert.assertTrue(extra == null || !extra.contains("XPlan"));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
