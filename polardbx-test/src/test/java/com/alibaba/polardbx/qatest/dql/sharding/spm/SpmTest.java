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
import com.google.common.truth.Truth;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
        executeBatchOnConn(tddlConnection, "baseline delete_all", null);

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
    public void baselineWithPushdownHint() {
        // clear current baseline info
        executeBatchOnConn(tddlConnection, "baseline delete_all", null);

        final String select = " select * from " + baseOneTableName
            + " where integer_test between 100 and 50 or integer_test between 500 and 233";

        String sql1 = "explain " + select;
        // test original sql plan ,making sure that it's a full table scan plan
        explainAllResultMatchAssert(sql1, null, tddlConnection, "[\\s\\S]*shardCount[\\s\\S]*");

        final String randomScanHint = "/*+TDDL:SCAN(\"" + baseOneTableName + "\", condition=\"pk = rand() * 100\")*/ ";
        String sql2 = "explain " + randomScanHint + select;

        // use hint make plan pushdown to a randomly chosen partition
        explainAllResultNotMatchAssert(sql2, null, tddlConnection, "[\\s\\S]*shardCount[\\s\\S]*");

        String sql3 = "baseline fix sql " + randomScanHint + select;

        // baseline fix plan with pushdown hint
        executeBatchOnConn(tddlConnection, sql3, null);

        // test original sql , whether its plan contains no shardCount
        explainAllResultNotMatchAssert(sql1, null, tddlConnection, "[\\s\\S]*shardCount[\\s\\S]*");

        // persist plan(not necessary)
        executeBatchOnConn(tddlConnection, "baseline PERSIST", null);

        String sql4 = "explain select * from " + baseOneTableName
            + " where integer_test between 1324 and 435 or integer_test between 345 and 222;";
        // test original sql with different params
        explainAllResultNotMatchAssert(sql4, null, tddlConnection, "[\\s\\S]*shardCount[\\s\\S]*");
    }

    @Test
    public void baselineWithPushdownHint1() {
        // clear current baseline info
        executeBatchOnConn(tddlConnection, "baseline delete_all", null);

        final String select = "select bigint_test from " + baseOneTableName
            + " where bigint_test in (10086) order by date_test desc limit 100";

        String sql1 = "explain " + select;
        // test original sql plan ,making sure that it's a full table scan plan
        explainAllResultMatchAssert(sql1, null, tddlConnection, "[\\s\\S]*shardCount[\\s\\S]*");

        final String randomScanHint = "/*+TDDL:SCAN(\"" + baseOneTableName + "\", condition=\"pk = rand() * 100\")*/ ";
        String sql2 = "explain " + randomScanHint + select;

        // use hint make plan pushdown to a randomly chosen partition
        explainAllResultNotMatchAssert(sql2, null, tddlConnection, "[\\s\\S]*shardCount[\\s\\S]*");

        String sql3 = "baseline fix sql " + randomScanHint + select;

        // baseline fix plan with pushdown hint
        executeBatchOnConn(tddlConnection, sql3, null);

        // test original sql , whether its plan contains no shardCount
        explainAllResultNotMatchAssert(sql1, null, tddlConnection, "[\\s\\S]*shardCount[\\s\\S]*");

        // persist plan(not necessary)
        executeBatchOnConn(tddlConnection, "baseline PERSIST", null);

        String sql4 = "select bigint_test from " + baseOneTableName
            + " where bigint_test in (10086, 9527, 27149) order by date_test desc limit 100";
        // test original sql with different params
        explainAllResultNotMatchAssert("explain " + sql4, null, tddlConnection, "[\\s\\S]*shardCount[\\s\\S]*");

        JdbcUtil.executeSuccess(tddlConnection, "trace " + sql4);
        List<String> showTraceResults = new ArrayList<>();
        try (final ResultSet showTraceRs = JdbcUtil.executeQuerySuccess(tddlConnection, "show trace")) {
            while (showTraceRs.next()) {
                showTraceResults.add(showTraceRs.getString("PARAMS"));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Truth.assertThat(showTraceResults).hasSize(1);
        Truth.assertThat(showTraceResults.get(0)).endsWith(", Raw(10086,9527,27149), 100]");
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

    @Test
    public void testBaselineRebuildAtLoad() throws SQLException {
        // clear current baseline info
        executeBatchOnConn(tddlConnection, "baseline delete_all", null);

        String sql = "baseline hint bind /*TDDL:a()*/ select * from " + baseOneTableName + " limit 100";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "select count(1) as hintCount from information_schema.spm where hint is not null";
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        int hintCount = rs.getInt("hintCount");
        rs.close();
        Assert.assertTrue(hintCount >= 1);

        // reload baseline
        sql = "baseline load";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "select count(1) as hintCount from information_schema.spm where hint is not null";
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        hintCount = rs.getInt("hintCount");
        rs.close();
        Assert.assertTrue(hintCount >= 1);
    }

    @Test
    public void testBaselineRebuildAtLoadPushDownHint() throws SQLException {
        // clear current baseline info
        executeBatchOnConn(tddlConnection, "baseline delete_all", null);

        String sql = "baseline add sql \n"
            + "/*+TDDL:SCAN(\n"
            + "    'select_base_two_multi_db_one_tb a, select_base_three_multi_db_one_tb b, select_base_four_multi_db_one_tb c', \n"
            + "    condition='a.pk = rand() * 10 and a.pk = b.pk and a.pk = c.pk')*/\n"
            + "select *\n"
            + "  from select_base_two_multi_db_one_tb a\n"
            + "  join select_base_three_multi_db_one_tb b\n"
            + "    on a.pk           = b.pk\n"
            + "  join select_base_four_multi_db_one_tb c\n"
            + "    on a.integer_test = c.integer_test;";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "select count(1) as hintCount from information_schema.spm where hint is not null";
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        int hintCount = rs.getInt("hintCount");
        rs.close();
        Assert.assertTrue(hintCount >= 1);

        // reload baseline
        sql = "baseline load";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "select count(1) as hintCount from information_schema.spm where hint is not null";
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        rs.next();
        hintCount = rs.getInt("hintCount");
        rs.close();
        Assert.assertTrue(hintCount >= 1);
    }

    @Test
    public void testBaselineRebuildAtLoadPushDownHintCurrentSchema() throws SQLException {
        // clear current baseline info
        executeBatchOnConn(tddlConnection, "baseline delete_all", null);
        String sql = "baseline add sql \n"
            + "/*+TDDL:SCAN(\n"
            + "    'select_base_two_multi_db_one_tb a, select_base_three_multi_db_one_tb b, select_base_four_multi_db_one_tb c', \n"
            + "    condition='a.pk = rand() * 10 and a.pk = b.pk and a.pk = c.pk')*/\n"
            + "select *\n"
            + "  from select_base_two_multi_db_one_tb a\n"
            + "  join select_base_three_multi_db_one_tb b\n"
            + "    on a.pk           = b.pk\n"
            + "  join select_base_four_multi_db_one_tb c\n"
            + "    on a.integer_test = c.integer_test;";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "trace select *\n"
            + "  from select_base_two_multi_db_one_tb a\n"
            + "  join select_base_three_multi_db_one_tb b\n"
            + "    on a.pk           = b.pk\n"
            + "  join select_base_four_multi_db_one_tb c\n"
            + "    on a.integer_test = c.integer_test";
        Statement statement = tddlConnection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        rs.close();
        sql = "show trace";
        rs = statement.executeQuery(sql);
        int rows = 0;
        while (rs.next()) {
            rows++;
        }
        rs.close();
        Assert.assertTrue(rows == 1);
    }

    @Test
    public void testBaselineRebuildAtLoadPushDownHintCrossSchema() throws SQLException {
        // clear current baseline info
        executeBatchOnConn(tddlConnection, "baseline delete_all", null);
        Statement statement = tddlConnection.createStatement();
        statement.executeQuery("use polardbx");
        String sql =
            "baseline fix SQL /*+TDDL:SCAN( 'drds_polarx1_part_qatest_app.select_base_two_multi_db_one_tb a, drds_polarx1_part_qatest_app.select_base_three_multi_db_one_tb b, drds_polarx1_part_qatest_app.select_base_four_multi_db_one_tb c', condition='a.pk = rand() * 10 and a.pk = b.pk and a.pk = c.pk')*/\n"
                + "SELECT *\n"
                + "FROM drds_polarx1_part_qatest_app.select_base_two_multi_db_one_tb a\n"
                + "JOIN drds_polarx1_part_qatest_app.select_base_three_multi_db_one_tb b ON a.pk = b.pk\n"
                + "JOIN drds_polarx1_part_qatest_app.select_base_four_multi_db_one_tb c ON a.integer_test = c.integer_test";
        statement.executeQuery(sql);
        sql = "trace SELECT *\n"
            + "FROM drds_polarx1_part_qatest_app.select_base_two_multi_db_one_tb a\n"
            + "JOIN drds_polarx1_part_qatest_app.select_base_three_multi_db_one_tb b ON a.pk = b.pk\n"
            + "JOIN drds_polarx1_part_qatest_app.select_base_four_multi_db_one_tb c ON a.integer_test = c.integer_test";
        ResultSet rs = statement.executeQuery(sql);
        rs.close();
        sql = "show trace";
        rs = statement.executeQuery(sql);
        int rows = 0;
        while (rs.next()) {
            rows++;
        }
        rs.close();
        Assert.assertTrue(rows == 1);
    }

    /**
     * Test baseline add/fix sql will merge IN operands with one '?'
     */
    @Test
    public void testBaselineWithInExpr() throws SQLException {
        String sqlAdd = "baseline fix sql /*TDDL:a()*/select pk from " + baseOneTableName + " where pk in (1, 3)";
        try (Statement statement = tddlConnection.createStatement()) {
            // get baseline id
            ResultSet rs = statement.executeQuery(sqlAdd);
            rs.next();
            String baselineId = rs.getString(1);
            rs.close();

            // delete baseline
            String sql = "baseline delete " + baselineId;
            statement.execute(sql);

            // re-fix it
            statement.execute(sqlAdd);

            // check PARAMETERIZED_SQL in spm if it contained only one question mark
            sql = "select PARAMETERIZED_SQL from information_schema.spm where BASELINE_ID=" + baselineId;
            rs = statement.executeQuery(sql);
            rs.next();
            String paramSql = rs.getString(1);
            rs.close();
            System.out.println(paramSql);
            Assert.assertTrue(paramSql.contains("(?)"));
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