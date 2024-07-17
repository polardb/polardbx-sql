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

package com.alibaba.polardbx.qatest.dml.sharding.gsi;

import com.alibaba.polardbx.qatest.BinlogIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.alibaba.polardbx.qatest.data.ExecuteTableName.HINT_STRESS_FLAG;
import static com.alibaba.polardbx.qatest.util.JdbcUtil.resultsSize;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultNotMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Test for index selection
 *
 * @author chenmo.cm
 */

@BinlogIgnore(ignoreReason = "用例涉及很多主键冲突问题，即不同分区有相同主键，复制到下游Mysql时出现Duplicate Key")
public class SelectGsiTest extends GsiDMLTest {

    @Parameterized.Parameters(name = "{index}:hint={0} table1={1} table2={2}")
    public static List<String[]> prepareData() {
        List<String[]> rets = doPrepareData();
        return rets;
    }

    public SelectGsiTest(String hint, String baseOneTableName, String baseTwoTableName) throws Exception {
        super(hint, baseOneTableName, baseTwoTableName);
    }

    @Before
    public void initData() throws Exception {
        super.initData();

        String sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseOneTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test,smallint_test)"
            + " values (?,?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList<List<Object>>();
        for (int i = 0; i < 20; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(i);
            param.add(i);
            param.add(i * 100);
            param.add("test" + i);
            param.add(columnDataGenerator.datetime_testValue);
            param.add(2000 + i);
            param.add(columnDataGenerator.char_testValue);
            param.add(i);

            params.add(param);
        }

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);

        sql = (HINT_STRESS_FLAG.equalsIgnoreCase(hint) ? hint + "insert " : "insert " + hint) + " into "
            + baseTwoTableName
            + " (pk,integer_test,bigint_test,varchar_test,datetime_test,year_test,char_test,smallint_test)"
            + " values (?,?,?,?,?,?,?,?)";

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, sql, params);
    }

    /**
     * Specify covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint1() {
        final String indexTableName = baseOneTableName.replaceFirst("base", "index1");
        final String sql =
            hint + "/*+TDDL:index(" + baseOneTableName + "," + indexTableName + ")*/ select integer_test from "
                + baseOneTableName;

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Specify non-covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint2() {
        final String indexTableName = baseOneTableName.replaceFirst("base", "index1");
        final String sql = hint + "/*+TDDL:index(" + baseOneTableName + "," + indexTableName + ")*/ select * from "
            + baseOneTableName;

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Specify covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint3() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");
        final String indexTableName2 = baseTwoTableName.replaceFirst("base", "index1");
        final String sql = MessageFormat.format(hint
                + "/*+TDDL:index({0}, {1}) index({2}, {3})*/ select a.integer_test from {4} a join {5} b on a.integer_test = b.integer_test",
            baseOneTableName,
            indexTableName1,
            baseTwoTableName,
            indexTableName2,
            baseOneTableName,
            baseTwoTableName);

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Specify covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint4() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");
        final String indexTableName2 = baseTwoTableName.replaceFirst("base", "index1");
        final String sql = MessageFormat.format(hint
                + "/*+TDDL:index(a, {0}) index(b, {1})*/ select a.integer_test from {2} a join {3} b on a.integer_test = b.integer_test",
            indexTableName1,
            indexTableName2,
            baseOneTableName,
            baseTwoTableName);

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Specify non-covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint5() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");
        final String indexTableName2 = baseTwoTableName.replaceFirst("base", "index1");
        final String sql = MessageFormat.format(hint
                + "/*+TDDL:index(a, {0}) index(b, {1})*/ select * from {2} a join {3} b on a.integer_test = b.integer_test",
            indexTableName1,
            indexTableName2,
            baseOneTableName,
            baseTwoTableName);

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Specify non-covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint6() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");
        final String sql = MessageFormat.format(hint + "/*+TDDL:index(a, {0})*/ select count(*) from {1} a",
            indexTableName1,
            baseOneTableName);

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Specify non-covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint7() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");
        final String sql =
            MessageFormat.format(hint + "/*+TDDL:index(a, {0})*/ select * from {1} a order by integer_test limit 1,3",
                indexTableName1,
                baseOneTableName);

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Specify non-covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint8() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");
        final String sql = MessageFormat.format(
            hint + "/*+TDDL:index(a, {0})*/ select * from {1} a where integer_test = 12 order by integer_test limit 3",
            indexTableName1,
            baseOneTableName);

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Specify non-covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint9() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");
        final String sql =
            MessageFormat.format(hint + "/*+TDDL:index(a, {0})*/ select * from {1} a order by integer_test limit 3",
                indexTableName1,
                baseOneTableName);

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Specify covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint10() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");
        final String indexTableName2 = baseTwoTableName.replaceFirst("base", "index1");
        final String sql = MessageFormat.format(hint
                + "/*+TDDL:index(a, {0}) index(b, {1})*/ select a.varchar_test, b.integer_test from {2} a join {3} b where a.integer_test = 0 and b.integer_test = 0 and a.bigint_test = 0 and b.bigint_test = 0",
            indexTableName1,
            indexTableName2,
            baseOneTableName,
            baseTwoTableName);

        explainAllResultNotMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection, "[\\s\\S]*" + "index1" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Specify covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint11() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");
        final String indexTableName2 = baseTwoTableName.replaceFirst("base", "index1");
        final String sql = MessageFormat.format(hint
                + "select {2}.varchar_test, b.integer_test from {2} force index({0}) join {3} b force index({1}) where {2}.integer_test = 0 and b.integer_test = 0 and {2}.bigint_test = 0 and b.bigint_test = 0",
            indexTableName1,
            indexTableName2,
            baseOneTableName,
            baseTwoTableName);

        explainAllResultNotMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection, "[\\s\\S]*" + "index1" + "[\\s\\S]*");
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);

        explainAllResultNotMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection, "[\\s\\S]*" + "index1" + "[\\s\\S]*");
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    /**
     * Specify covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint12() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");
        final String indexTableName2 = baseTwoTableName.replaceFirst("base", "index1");
        final String sql = MessageFormat.format(hint
                + "select {2}.varchar_test, b.integer_test from {2} force index({0}) join {3} b force index({1}) where {2}.integer_test = 0 and b.integer_test = 0 and {2}.bigint_test = 0 and b.bigint_test = 0",
            "a",
            "b",
            baseOneTableName,
            baseTwoTableName);

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        explainAllResultNotMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "FORCE INDEX\\(" + "[\\s\\S]*");
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection, "[\\s\\S]*" + "index1" + "[\\s\\S]*");
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    /**
     * Specify covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint13() {
        final String sql = MessageFormat.format(hint
                + "/*+TDDL:CMD_EXTRA(ENABLE_INDEX_SELECTION=FALSE)*/ select {2}.varchar_test, b.integer_test from {2} force index({0}) join {3} b force index({1}) where {2}.integer_test = 0 and b.integer_test = 0 and {2}.bigint_test = 0 and b.bigint_test = 0",
            "PRIMARY",
            "b",
            baseOneTableName,
            baseTwoTableName);
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    /**
     * Specify covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithHint14() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");
        final String indexTableName2 = baseTwoTableName.replaceFirst("base", "index1");
        final String sql = MessageFormat.format(hint
                + "/*+TDDL:INDEX({4}, {5}) CMD_EXTRA(ENABLE_INDEX_SELECTION=FALSE)*/ select {2}.varchar_test, b.integer_test from {2} force index({0}) join {3} b force index({1}) where {2}.integer_test = 0 and {2}.pk = 0 and {2}.varchar_test = 0 and b.integer_test = 0 and {2}.bigint_test = 0 and b.bigint_test = 0",
            "PRIMARY",
            "b",
            baseOneTableName,
            baseTwoTableName,
            baseTwoTableName,
            indexTableName2);

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "PhyTableOperation\\(" + "[\\s\\S]*");
        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "FORCE INDEX\\(" + "[\\s\\S]*");

        final List<Pair<String, String>> topology = JdbcUtil.getTopologyWithHint(tddlConnection, indexTableName2, hint);
        String physicalIndexTableName = topology.get(0).right.substring(0, topology.get(0).right.length() - 3);

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + physicalIndexTableName + "[\\s\\S]*");
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    private Map<String, String> getTableToIndexKey() {
        Map<String, String> tableToIndexKey = new HashMap<>();
        tableToIndexKey.put("gsi_dml_unique_one_index_base", "integer_test");
        tableToIndexKey.put("gsi_dml_unique_multi_index_base", "integer_test");
        tableToIndexKey.put("gsi_dml_no_unique_one_index_base", "integer_test");
        tableToIndexKey.put("gsi_dml_no_unique_multi_index_base", "integer_test");
        tableToIndexKey.put("gsi_dml_composite_unique_one_index_base", "integer_test");
        tableToIndexKey.put("gsi_dml_composite_unique_multi_index_base", "integer_test");
        tableToIndexKey.put("gsi_dml_global_unique_one_index_base", "bigint_test");
        tableToIndexKey.put("gsi_dml_unique_one_index_base", "integer_test");
        tableToIndexKey.put("gsi_dml_unique_multi_index_base", "integer_test");
        tableToIndexKey.put("gsi_dml_no_unique_one_index_base", "bigint_test");
        tableToIndexKey.put("gsi_dml_no_unique_multi_index_base", "integer_test");
        tableToIndexKey.put("gsi_dml_composite_unique_one_index_base", "integer_test");
        tableToIndexKey.put("gsi_dml_composite_unique_multi_index_base", "integer_test");
        tableToIndexKey.put("gsi_dml_global_unique_one_index_base", "bigint_test");
        tableToIndexKey.put("gsi_dml_no_unique_one_index_mp_base", "bigint_test");
        tableToIndexKey.put("gsi_dml_no_unique_one_index_mpk_base", "bigint_test");
        return tableToIndexKey;
    }

    @Test
    public void CBOGsiAsBKALookupSide() {

        Map<String, String> tableToIndexKey = getTableToIndexKey();

        String driveTableName = "select_base_one_multi_db_multi_tb";

        String gsiName = baseOneTableName.replaceFirst("base", "index1");

        final String hint =
            "/*+TDDL: index(" + baseOneTableName + "," + gsiName + ")  "
                + "BKA_JOIN(" + driveTableName + ",(" + gsiName + "," + baseOneTableName + "))*/ ";
        final String sql =
            hint + " select t1.integer_test,t2.integer_test,t1.pk,t2.pk,t2.year_test from " + driveTableName
                + " t1 join "
                + baseOneTableName + " t2 on t1"
                + "." + tableToIndexKey
                .get(baseOneTableName) + " = t2." + tableToIndexKey.get(baseOneTableName) + "";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void CBOGsiAsSemiBKALookupSide() {

        Map<String, String> tableToIndexKey = getTableToIndexKey();

        String driveTableName = "select_base_one_multi_db_multi_tb";

        String gsiName = baseOneTableName.replaceFirst("base", "index1");

        final String hint =
            "/*+TDDL: index(" + baseOneTableName + "," + gsiName + ")  "
                + "SEMI_BKA_JOIN(" + driveTableName + ",(" + gsiName + "," + baseOneTableName + "))*/ ";
        final String sql =
            hint + " select t1.integer_test,t1.pk,t1.year_test from " + driveTableName + " t1 where t1."
                + tableToIndexKey
                .get(baseOneTableName) + " in ( select t2." + tableToIndexKey
                .get(baseOneTableName) + " from "
                + baseOneTableName + " t2 where t2.year_test is not null);";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void CBOGsiAsMaterializedSemiLookupSide() {

        Map<String, String> tableToIndexKey = getTableToIndexKey();

        String driveTableName = "select_base_one_multi_db_multi_tb";

        String gsiName = baseOneTableName.replaceFirst("base", "index1");

        final String hint =
            "/*+TDDL: index(" + baseOneTableName + "," + gsiName + ")  "
                + "MATERIALIZED_SEMI_JOIN((" + gsiName + "," + baseOneTableName + ")," + driveTableName + ")*/ ";
        final String sql =
            hint + " select t1.integer_test,t1.pk,t1.year_test from " + baseOneTableName + " t1 where t1."
                + tableToIndexKey
                .get(baseOneTableName) + " in ( select t2." + tableToIndexKey
                .get(baseOneTableName) + " from "
                + driveTableName + " t2 where t2.year_test is not null);";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Ignore("与开发确认可以删除")
    @Test
    public void CBOCoveringIndexSelection() {

        Map<String, String> tableToIndexKey = getTableToIndexKey();

        JdbcUtil.executeUpdateSuccess(tddlConnection, "analyze table " + baseOneTableName);

        final String avoidPlanCacheHint = "/*+TDDL:cmd_extra()*/ ";
        final String sql =
            avoidPlanCacheHint + " select integer_test from " + baseOneTableName + " where " + tableToIndexKey
                .get(baseOneTableName) + " = 0";

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        final String sql2 =
            avoidPlanCacheHint + "select integer_test from " + baseOneTableName + " where " + tableToIndexKey
                .get(baseOneTableName) + " = 0 order by " + tableToIndexKey.get(baseOneTableName) + " limit 1";

        explainAllResultMatchAssert("EXPLAIN " + sql2, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        final String sql3 =
            avoidPlanCacheHint + "select integer_test from " + baseOneTableName + " group by " + tableToIndexKey
                .get(baseOneTableName) + " order by " + tableToIndexKey.get(baseOneTableName);

        explainAllResultMatchAssert("EXPLAIN " + sql3, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);

        final String sql4 =
            avoidPlanCacheHint + "select a.integer_test,b.integer_test from " + baseOneTableName + " a join "
                + baseOneTableName + " b on a." + tableToIndexKey.get(baseOneTableName) + " = b." + tableToIndexKey
                .get(baseOneTableName);

        explainAllResultMatchAssert("EXPLAIN " + sql4, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql4, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void CBOUnCoveringIndexSelection() {

        Map<String, String> tableToIndexKey = getTableToIndexKey();

        JdbcUtil.executeUpdateSuccess(tddlConnection, "analyze table " + baseOneTableName);

        final String avoidPlanCacheHint = "/*+TDDL:cmd_extra()*/ ";
        final String sql =
            avoidPlanCacheHint + " select * from " + baseOneTableName + " where " + tableToIndexKey
                .get(baseOneTableName) + " = 0";

        explainAllResultMatchAssert("EXPLAIN " + sql, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        final String sql2 =
            avoidPlanCacheHint + "select * from " + baseOneTableName + " where " + tableToIndexKey
                .get(baseOneTableName) + " = 0 order by " + tableToIndexKey.get(baseOneTableName);

        explainAllResultMatchAssert("EXPLAIN " + sql2, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        final String sql3 =
            avoidPlanCacheHint + "select * from " + baseOneTableName + " where " + tableToIndexKey
                .get(baseOneTableName) + " = 0 order by " + tableToIndexKey.get(baseOneTableName) + " limit 1";

        explainAllResultMatchAssert("EXPLAIN " + sql3, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);

        final String sql4 =
            avoidPlanCacheHint + "select * from " + baseOneTableName + " a join "
                + baseOneTableName + " b on a." + tableToIndexKey.get(baseOneTableName) + " = b." + tableToIndexKey
                .get(baseOneTableName) + " where a." + tableToIndexKey.get(baseOneTableName) + " = 0";

        explainAllResultMatchAssert("EXPLAIN " + sql4, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql4, null, mysqlConnection, tddlConnection);

        final String sql5 =
            avoidPlanCacheHint + " select * from " + baseOneTableName + " where " + tableToIndexKey
                .get(baseOneTableName) + " in (0,1)";

        explainAllResultMatchAssert("EXPLAIN " + sql5, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql5, null, mysqlConnection, tddlConnection);

        final String sql6 =
            avoidPlanCacheHint + "select * from " + baseOneTableName + " where " + tableToIndexKey
                .get(baseOneTableName) + " in (1,0) order by " + tableToIndexKey.get(baseOneTableName);

        explainAllResultMatchAssert("EXPLAIN " + sql6, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql6, null, mysqlConnection, tddlConnection);

        final String sql7 =
            avoidPlanCacheHint + "select * from " + baseOneTableName + " where " + tableToIndexKey
                .get(baseOneTableName) + " in (1,0) order by " + tableToIndexKey.get(baseOneTableName) + " limit 1";

        explainAllResultMatchAssert("EXPLAIN " + sql7, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql7, null, mysqlConnection, tddlConnection);

        final String sql8 =
            avoidPlanCacheHint + "select * from " + baseOneTableName + " a join "
                + baseOneTableName + " b on a." + tableToIndexKey.get(baseOneTableName) + " = b." + tableToIndexKey
                .get(baseOneTableName) + " where a." + tableToIndexKey.get(baseOneTableName) + " in (0,1)";

        explainAllResultMatchAssert("EXPLAIN " + sql8, null, tddlConnection,
            "[\\s\\S]*" + "IndexScan\\(" + "[\\s\\S]*");
        selectContentSameAssert(sql8, null, mysqlConnection, tddlConnection);
    }

    /**
     * Specify non-covering index with HINT, assert result content identical with mysql
     */
    @Test
    public void indexSelectionWithBKAPruning() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");

        // Prune to one table.
        for (int i = 0; i < 20; ++i) {
            final String sql = MessageFormat.format(
                hint
                    + "/*+TDDL: ENABLE_BKA_PRUNING=true index(a, {0}) bka_join({0}, {1})*/ select * from {1} a where bigint_test = {2}",
                indexTableName1,
                baseOneTableName,
                Long.toString(i * 100));

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

            if (hint.isEmpty()) {
                // Use trace number to test pruning state.
                JdbcUtil.executeQuery("trace " + sql, tddlConnection);
                final long bkaCount =
                    getTrace(tddlConnection).stream().filter(line -> line.get(9).contains(" IN ")).count();
                if (!baseOneTableName.equals("gsi_dml_no_unique_one_index_mpk_base") && bkaCount > 0) {
                    Assert.assertEquals("Bad BKA pruning.", 1, bkaCount);
                }
            }
        }

        // Prune to one or two table.
        Random random = new Random();
        for (int i = 0; i < 20; ++i) {
            final String sql = MessageFormat.format(
                "/*+TDDL: ENABLE_BKA_PRUNING=true index(a, {0}) bka_join({0}, {1})*/ select * from {1} a where bigint_test = {2} or bigint_test = {3}",
                indexTableName1,
                baseOneTableName,
                Long.toString(i * 100),
                Long.toString(random.nextInt(20) * 100));

            selectContentSameAssert(hint + sql, null, mysqlConnection, tddlConnection);

            if (hint.isEmpty()) {
                // Use trace number to test pruning state.
                JdbcUtil.executeQuery("trace " + sql, tddlConnection);
                final long bkaCount =
                    getTrace(tddlConnection).stream().filter(line -> line.get(9).contains(" IN ")).count();
                if (!baseOneTableName.equals("gsi_dml_no_unique_one_index_mpk_base") && bkaCount > 0) {
                    Assert.assertTrue("Bad BKA pruning.", bkaCount <= 4);
                }
            }
        }
    }

    @Test
    public void indexSelectionWithBKAPruningUnion() {
        final String indexTableName1 = baseOneTableName.replaceFirst("base", "index1");

        // Prune to one table.
        for (int i = 0; i < 20; ++i) {
            final String sql = MessageFormat.format(
                hint
                    + "/*+TDDL: ENABLE_BKA_PRUNING=true index(a, {0}) bka_join({0}, {1}) cmd_extra(MERGE_UNION_SIZE=0)*/ select * from {1} a where bigint_test = {2}",
                indexTableName1,
                baseOneTableName,
                Long.toString(i * 100));

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

            if (hint.isEmpty()) {
                // Use trace number to test pruning state.
                JdbcUtil.executeQuery("trace " + sql, tddlConnection);
                final long bkaCount =
                    getTrace(tddlConnection).stream().filter(line -> line.get(9).contains(" IN ")).count();
                if (!baseOneTableName.equals("gsi_dml_no_unique_one_index_mpk_base") && bkaCount > 0) {
                    Assert.assertEquals("Bad BKA pruning.", 1, bkaCount);
                }
            }
        }

        // Prune to one or two table.
        Random random = new Random();
        for (int i = 0; i < 20; ++i) {
            final String sql = MessageFormat.format(
                hint
                    + "/*+TDDL: ENABLE_BKA_PRUNING=true index(a, {0}) bka_join({0}, {1}) cmd_extra(MERGE_UNION_SIZE=0)*/ select * from {1} a where bigint_test = {2} or bigint_test = {3}",
                indexTableName1,
                baseOneTableName,
                Long.toString(i * 100),
                Long.toString(random.nextInt(20) * 100));

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

            if (hint.isEmpty()) {
                // Use trace number to test pruning state.
                JdbcUtil.executeQuery("trace " + sql, tddlConnection);
                final long bkaCount =
                    getTrace(tddlConnection).stream().filter(line -> line.get(9).contains(" IN ")).count();
                if (!baseOneTableName.equals("gsi_dml_no_unique_one_index_mpk_base") && bkaCount > 0) {
                    Assert.assertTrue("Bad BKA pruning.", 1 == bkaCount || 2 == bkaCount);
                }
            }
        }
    }

    /**
     * Specify non-covering index with HINT, check explain executeSuccess
     */
    @Test
    public void explainExecuteSupportIndexScan() {
        final String indexTableName = baseOneTableName.replaceFirst("base", "index1");
        final String sql = hint + "/*+TDDL:index(" + baseOneTableName + "," + indexTableName + ")*/ select * from "
            + baseOneTableName;

        Assert.assertTrue(
            resultsSize(JdbcUtil.executeQuerySuccess(tddlConnection, "EXPLAIN EXECUTE" + sql)) > 0);
    }
}
