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

package com.alibaba.polardbx.qatest.dql.sharding.explain;

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

/**
 * @author hongxi.chx
 */

public class ExplainTest extends ReadBaseTestCase {

    private static final Log log = LogFactory.getLog(ExplainTest.class);

    public ExplainTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectOneTableMultiRuleMode());
    }

    /**
     * @since 5.4.5
     */
    @Test
    public void explainSelectTest() {
        String sql =
            "/* ApplicationName=IntelliJ IDEA 2019.3.1 */ explain select * from " + baseOneTableName + " a inner join "
                + baseTwoTableName + " b where a.pk = b.pk";
        try (Statement statement = tddlConnection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            if (existsMultiDbMultiTb(baseOneTableName, baseTwoTableName)) {
                Assert.fail("need exception here");
            }
            Assert.assertTrue(rs.next());
        } catch (Exception e) {
            log.error(e.getMessage());
            if (!existsMultiDbMultiTb(baseOneTableName, baseTwoTableName)) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * @since 5.4.5
     */
    @Test
    public void explainSelectTest1() {
        String sql1 =
            "/* ApplicationName=IntelliJ IDEA 2019.3.1 */ /*+TDDL:node(0)*/explain select * from " + baseOneTableName
                + " a inner join " + baseTwoTableName + " b where a.pk = b.pk";
        String sql2 = "explain/*+TDDL:node(0)*/ select * from " + baseOneTableName + " a inner join " + baseTwoTableName
            + " b where a.pk = b.pk";
        try (Statement statement1 = tddlConnection.createStatement();
            Statement statement2 = tddlConnection.createStatement()) {
            ResultSet rs1 = statement1.executeQuery(sql1);
            ResultSet rs2 = statement2.executeQuery(sql2);
            if (existsMultiDbMultiTb(baseOneTableName, baseTwoTableName)) {
                Assert.fail("need exception here");
            }
            if (rs1.next() && rs2.next()) {
                final String string1 = rs1.getString(1);
                final String string2 = rs2.getString(1);
                Assert.assertTrue(!string1.equals(string2));
            }

        } catch (Exception e) {
            log.error(e.getMessage());
            if (!existsMultiDbMultiTb(baseOneTableName, baseTwoTableName)) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * @since 5.4.5
     */
    @Test
    public void selectTest() {
        String sql = "/* ApplicationName=IntelliJ IDEA 2019.3.1 */ /*+TDDL master()*/ select * from " + baseOneTableName
            + " a inner join " + baseTwoTableName + " b where a.pk = b.pk";
        try (Statement statement = tddlConnection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            Assert.assertTrue(rs.next());
            if (existsMultiDbMultiTb(baseOneTableName, baseTwoTableName)) {
                Assert.fail("need exception here");
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            if (!existsMultiDbMultiTb(baseOneTableName, baseTwoTableName)) {
                throw new RuntimeException(e);
            }
        }

    }

    private boolean existsMultiDbMultiTb(String... tables) {
        for (int i = 0; i < tables.length; i++) {
            if (tables[i].contains("multi_db_multi_tb")) {
                return true;
            }
        }
        return false;
    }

}
