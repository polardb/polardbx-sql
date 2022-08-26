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

package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isMySQL80;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentLengthSameAssert;

/**
 * union查询测试
 *
 * @author jianghang 2014-8-5 上午11:31:37
 * @since 5.1.0
 */

public class SelectWithUnionTest extends ReadBaseTestCase {

    private boolean inTrans = false;

    @Parameters(name = "{index}:table0={0},table1={1},inTrans={2}")
    public static List<String[]> prepare() {
        return Arrays.asList(selectBaseOneBaseTwoWithTrans());
    }

    /**
     * 查询两张表情况，多种模式
     */
    public static String[][] selectBaseOneBaseTwoWithTrans() {

        String[][] object = {
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "false"},
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "false"},
            {
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "false"},

            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "false"},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "false"},
            //in trans

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "true"},
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "true"},
            {
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "true"},

            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "true"},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "true"},
        };
        return object;

    }

    public SelectWithUnionTest(String baseOneTableName, String baseTwoTableName, String intrans) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
        this.inTrans = Boolean.valueOf(intrans);
    }

    private void selectContentSameAssert(String sql, List<Object> param, Connection mysqlConnection,
                                         Connection tddlConnection) throws Exception {
        if (inTrans) {
            tddlConnection.setAutoCommit(false);
        }
        DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (inTrans) {
            tddlConnection.commit();
            tddlConnection.setAutoCommit(true);
        }
    }

    /**
     * @since 5.1.10
     */
    @Test
    public void unionTest() throws Exception {

        String sql = "/*+TDDL({'extra':{'CHOOSE_UNION_USE_ALL':'FALSE'}})*/select integer_test,varchar_test from "
            + baseOneTableName + " union select pk,varchar_test from " + baseTwoTableName;

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.10
     */
    @Test
    public void unionAliasTest() throws Exception {

        String sql =
            "/*+TDDL({'extra':{'CHOOSE_UNION_USE_ALL':'FALSE'}})*/select a.integer_test as aid,a.varchar_test aname from "
                + baseOneTableName
                + " a union select b.pk as bid ,b.varchar_test as bname from "
                + baseTwoTableName + " b";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.10
     */
    @Test
    public void unionSingleTest() throws Exception {

        String sql = "/*+TDDL({'extra':{'CHOOSE_UNION_USE_ALL':'FALSE'}})*/select pk,varchar_test from "
            + baseOneTableName + " where integer_test = 1 union select pk,varchar_test from "
            + baseTwoTableName + " where pk = 1 ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.10
     */
    @Test
    public void unionAllTest() throws Exception {

        String sql = "select integer_test,varchar_test from " + baseOneTableName
            + " union all select pk,varchar_test from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.10
     */
    @Test
    public void unionDistinctTest() throws Exception {

        String sql = "select integer_test,varchar_test from " + baseOneTableName
            + " union distinct select pk,varchar_test from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.10
     */
    @Test
    public void unionOrderbyTest() throws Exception {

        String sql = "(select integer_test,varchar_test from " + baseOneTableName
            + ") union distinct (select pk,varchar_test from " + baseTwoTableName + " ) order by varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.10
     */
    @Test
    public void unionSingleOrderbyTest() throws Exception {

        String sql = "(select pk ,varchar_test from " + baseOneTableName
            + " where integer_test = 1) union all (select pk,varchar_test from " + baseTwoTableName
            + " where pk = 1 ) order by pk limit 2,10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.10
     */
    @Ignore
    public void unionDiffTypeTest() throws Exception {

        String sql = "(select integer_test,varchar_test as name1 from " + baseOneTableName
            + ") union all (select varchar_test,pk as id1 from " + baseTwoTableName + " )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.10
     */
    @Ignore
    public void unionDiffTypeOrderTest() throws Exception {

        String sql = "(select integer_test,varchar_test as name1 from " + baseOneTableName
            + ") union distinct (select varchar_test,pk as id1 from " + baseTwoTableName + " ) order by name1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    @Ignore("暂不支持重复列名")
    public void unionSameColumnTest() throws Exception {

        String sql = "(select integer_test,integer_test, 'integer_test', `integer_test`, \"integer_test\" from "
            + baseOneTableName + ") union all (select pk,pk,'pk', `pk`, \"pk\" from " + baseTwoTableName
            + " )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.3.10
     */
    @Test
    public void unionAllMoreTwoTableTest() throws Exception {

        String sql = "select integer_test from " + baseOneTableName + " union all select integer_test from "
            + baseTwoTableName + " union all select integer_test from " + baseTwoTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.3.10
     */
    @Test
    public void unionAllMoreTwoTableTest2() throws Exception {
        if (isMySQL80()) {// Why filter out it in MySQL8, this is a tough
            // question to explain
            return;
        }

        String sql = "select * from ( select tinyint_1bit_test,bit_test,pk from " + baseOneTableName
            + " union select tinyint_1bit_test,bit_test,pk from " + baseOneTableName + ") b  order by pk limit 2";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.10
     */
    @Test
    public void unionInSubqueryTest() throws Exception {

        String sql = "select integer_test,varchar_test from " + baseOneTableName
            + " where (integer_test,varchar_test) in (" + "select integer_test,varchar_test from "
            + baseOneTableName + " union select pk,varchar_test from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.10
     */
    @Test
    public void unionWithSubqueryTest() throws Exception {

        String sql = " select * from (" + "select integer_test,varchar_test from " + baseOneTableName
            + " union distinct select pk,varchar_test from " + baseTwoTableName
            + ") a where integer_test > 1 order by integer_test,varchar_test desc";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.10
     */
    @Test
    public void unionWithJoinTest() throws Exception {

        String sql = "select a.integer_test , b.varchar_test from (" + "select integer_test,varchar_test from "
            + baseOneTableName + " union distinct select pk,varchar_test from " + baseTwoTableName
            + ") a inner join " + baseOneTableName
            + " b on a.integer_test = b.integer_test where a.integer_test > 1 order by a.integer_test desc";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.10
     */
    @Test
    public void unionWithMixedOptionTest() throws Exception {

        // a union all b union distinct c union all d
        String sql = "select integer_test,varchar_test from " + baseOneTableName
            + " where integer_test > 100 union all select integer_test,varchar_test from " + baseTwoTableName
            + " where pk > 100 union distinct select integer_test,varchar_test from " + baseOneTableName
            + " where integer_test < 50 union all select integer_test,varchar_test from " + baseTwoTableName
            + " where pk < 50 ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void unionWithLimitTest() throws Exception {

        String sql = String.format("select pk,varchar_test from %s where integer_test>15 or integer_test<5 union all "
                + "select pk,varchar_test from %s where 8<pk and pk<12 order by pk limit 10",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void unionAllSelectLimitTest() throws Exception {

        String sql = String.format("select pk,varchar_test from %s where integer_test>15 or integer_test<5 union all "
                + "select pk,varchar_test from %s where 8<pk and pk<12 order by pk limit 10",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void unionAllSelectWithLimitTest() throws Exception {
        String sql =
            String.format("select integer_test,varchar_test from %s where integer_test>15 or integer_test<5 union all "
                    + "(select pk,varchar_test from %s where 8<pk and pk<12 order by pk limit 10)",
                baseOneTableName,
                baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void unionAllLimitSelectWithLimitOrderbyLimitTest() throws Exception {

        String sql = String.format("select pk,varchar_test from %s where integer_test>15 or integer_test<5 union all "
                + "(select pk,varchar_test from %s where 8<pk and pk<12 order by pk limit 10) order by pk limit 13",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void unionDistinctLimitTest() throws Exception {

        String sql =
            String.format("select pk,varchar_test from %s where integer_test>15 or integer_test<5 union distinct "
                    + "select pk,varchar_test from %s where 8<pk and pk<12 order by pk limit 10",
                baseOneTableName,
                baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void unionDistinctSelectLimitTest() throws Exception {

        String sql = String
            .format("select integer_test,varchar_test from %s where integer_test>15 or integer_test<5 union distinct "
                    + "(select pk,varchar_test from %s where 8<pk and pk<12 order by pk limit 10)",
                baseOneTableName,
                baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    public void unionDistinctLimitSelectLimitTest() throws Exception {

        String sql =
            String.format("select pk,varchar_test from %s where integer_test>15 or integer_test<5 union distinct "
                    + "(select pk,varchar_test from %s where 8<pk and pk<12 order by pk limit 10) order by pk,varchar_test limit 13",
                baseOneTableName,
                baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    @Ignore("does not support group_concat")
    public void unionGroupConcatSelectLimitTest() throws Exception {

        String sql = String.format("select group_concat(pk,varchar_test) from %s where integer_test=7 union"
                + "(select group_concat(pk,varchar_test) as gr  from %s where 8<pk and pk<12 order by gr limit 10) ",
            baseOneTableName,
            baseTwoTableName);
        selectContentLengthSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    @Ignore("does not support group_concat")
    public void unionGroupConcatOrderbySelectLimitTest() throws Exception {

        // group_concat 只支持单表情况
        if (baseOneTableName.contains(ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX)
            || baseOneTableName.contains(ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX)) {
            return;
        }

        String sql = String
            .format("select group_concat(pk,varchar_test order by pk) as idname from %s where integer_test=71  union"
                    + "(select group_concat(pk,varchar_test) as gr from %s where 8<pk and pk<12 order by gr limit 10) ",
                baseOneTableName,
                baseTwoTableName);
        selectContentLengthSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-SNAPSHOT
     */
    @Test
    public void unionOrderByNumberTest() throws Exception {
        String sql =
            String.format("select pk,varchar_test from %s where integer_test>15 or integer_test<5 union distinct "
                    + "(select pk,varchar_test from %s where 8<pk and pk<12 order by 1 limit 10) order by 1,2 limit 13",
                baseOneTableName,
                baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.27-SNAPSHOT
     */
    @Test
    public void unionOutOfRangeValueSumTest() throws Exception {

        String sql = String.format("select sum(integer_test + 2147483647) as amount1, 0 as amount2 from %s union all "
                + "select 0 as amount1, sum(integer_test + 2147483647) as amount2 from %s",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.27-SNAPSHOT
     */
    @Test
    public void unionOutOfRangeValueAddTest() throws Exception {

        String sql = String.format("select integer_test + 2147483647 as amount1, 0 as amount2 from %s union all "
                + "select 0 as amount1, integer_test + 2147483647 as amount2 from %s",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.27-SNAPSHOT
     */
    @Test
    public void unionOutOfRangeValueSumAddTest() throws Exception {

        String sql = String.format("select sum(integer_test) + 2147483647 as amount1, 0 as amount2 from %s union all "
                + "select 0 as amount1, sum(integer_test) + 2147483647 as amount2 from %s",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.3.10-SNAPSHOT
     */
    @Test
    public void unionWithRightLimitWithoutParen() throws Exception {

        String sql = String.format("select integer_test from %s where pk = 17"
                + " union select integer_test from %s where pk = 17 limit 1",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.3.13-SNAPSHOT
     */
    @Test
    public void unionWithLimit() throws Exception {

        String sql =
            "SELECT SUM(found) FROM ((SELECT 1 as found FROM information_schema.tables WHERE table_schema='g_test') UNION ALL (SELECT 1 as found FROM information_schema.views WHERE table_schema='g_test' LIMIT 1) UNION ALL (SELECT 1 as found FROM information_schema.table_constraints WHERE table_schema='g_test' LIMIT 1) UNION ALL (SELECT 1 as found FROM information_schema.events WHERE event_schema='g_test' LIMIT 1) UNION ALL (SELECT 1 as found FROM information_schema.triggers WHERE trigger_schema='g_test' LIMIT 1) UNION ALL (SELECT 1 as found FROM information_schema.routines WHERE routine_schema='g_test' LIMIT 1)) as all_found;";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.3.13-SNAPSHOT
     */
    @Test
    public void subQueryUnionWithLimit() throws Exception {
        //参见case unionAllMoreTwoTableTest2，需要类型系统支持
        if (isMySQL80()) {// Why filter out it in MySQL8, this is a tough
            // question to explain
            return;
        }

        String sql = String.format(
            "SELECT * FROM (select * from %s where pk = 17 union all select * from %s where pk = 17 limit 1) a;",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
