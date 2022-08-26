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

package com.alibaba.polardbx.qatest.dql.sharding.spm;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnConn;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultNotMatchAssert;

/**
 * test spm
 *
 * @author roy
 */

@Ignore("多并发测试，当前SPMTest逻辑不适用于多并发测试")
public class SpmTest extends ReadBaseTestCase {

    private static final Log log = LogFactory.getLog(SpmTest.class);

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1},table2={2}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableName.gsiDMLTableSimplfy());
    }

    public SpmTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainSelectTest() {

        if (usingNewPartDb()) {
            // new part db has no gsi gsi_dml_unique_one_index_index1, so ignore the case
            return;
        }

        // clear current baseline info
        executeBatchOnConn(tddlConnection, "baseline clear", null);

        String sql1 = "explain select * from " + baseOneTableName
            + " where integer_test between 100 and 50 or integer_test between 500 and 233;";
        // test original sql plan ,making sure that it contains no plan
        explainAllResultNotMatchAssert(sql1, null, tddlConnection, "[\\s\\S]*IndexScan[\\s\\S]*");

        String sql2 = "explain /*TDDL:index(" + baseOneTableName + ",gsi_dml_unique_one_index_index1)*/ select * from "
            + baseOneTableName + " where integer_test between 100 and 50 or integer_test between 500 and 233";

        // use hint make plan using index
        explainAllResultMatchAssert(sql2, null, tddlConnection, "[\\s\\S]*IndexScan[\\s\\S]*");

        String sql3 =
            "baseline fix sql /*TDDL:index(" + baseOneTableName + ",gsi_dml_unique_one_index_index1)*/ select * from "
                + baseOneTableName + " where integer_test between 100 and 50 or integer_test between 500 and 233";

        // baseline fix plan with index
        executeBatchOnConn(tddlConnection, sql3, null);

        // test orignal sql , if its plan contains index
        explainAllResultMatchAssert(sql1, null, tddlConnection, "[\\s\\S]*IndexScan[\\s\\S]*");

        // persist plan(not necessary)
        executeBatchOnConn(tddlConnection, "baseline PERSIST", null);

        String sql4 = "explain select * from " + baseOneTableName
            + " where integer_test between 1324 and 435 or integer_test between 345 and 222;";
        // test orignal sql with different params
        explainAllResultMatchAssert(sql4, null, tddlConnection, "[\\s\\S]*IndexScan[\\s\\S]*");
    }

    @Test
    public void testPlanCacheFreshAfterAnalyzeTable() throws SQLException {
        //bypass selectHandler
        String sql = "show tables";
        String targetTable = JdbcUtil.executeQueryAndGetFirstStringResult(sql, tddlConnection);
//        System.out.println(targetTable);
        sql = "select * from " + targetTable + " limit 100";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        List<String> tableNames = getPlanCacheTableNames();

        if (!tableNames.contains(targetTable)) {
            Assert.fail("plan cache should have the latest query");
        }

        // analyze table
        sql = "analyze table " + targetTable;
        JdbcUtil.executeSuccess(tddlConnection, sql);

        List<String> newTableNames = getPlanCacheTableNames();

        if (newTableNames.contains(targetTable)) {
            Assert.fail("plan cache should not contains the table name of the latest query after analyze");
        }
    }

    private List<String> getPlanCacheTableNames() throws SQLException {
        ResultSet resultSet =
            JdbcUtil.executeQuery("select TABLE_NAMES  from information_schema.plan_cache", tddlConnection);

        List<String> tableNames = Lists.newLinkedList();
        while (resultSet.next()) {
            tableNames.add(resultSet.getString("TABLE_NAMES"));
        }

        resultSet.close();
        return tableNames;
    }
}