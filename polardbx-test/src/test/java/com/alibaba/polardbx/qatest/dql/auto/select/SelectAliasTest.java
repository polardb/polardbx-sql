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

package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * 别名测试
 *
 * @author zhuoxue.yll
 * @since 5.0.1
 * 2016年10月26日
 */

public class SelectAliasTest extends AutoReadBaseTestCase {

    private ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwo());
    }

    public SelectAliasTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasTableTest() {
        String sql = String.format("select * from  %s nor where nor.pk=?", baseOneTableName);
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.pkValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasScalarTest() {
        String sql = "select 1 as id from  " + baseOneTableName + " nor limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasScalarInSubQueryTest() {
        String sql1 = "select id from (select 1 as id from " + baseOneTableName + " nor limit 1) x";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void constantsInSubQueryTest() {
        String sql2 = "select * from (select 1 from " + baseOneTableName + " nor limit 1) x";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasTableTestWithIndexQuery() {
        String sql = "select * from  " + baseOneTableName + "  nor where nor.integer_test =? order by pk limit 10 ";
        List<Object> param = new ArrayList<Object>();
        param.add(columnDataGenerator.integer_testValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasWithAsTableTest() {
        String sql = "select * from  " + baseOneTableName + "  as nor where nor.pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);

        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasFiledTest() {
        String sql =
            "select varchar_test xingming ,integer_test  pid ,pk ppk from  " + baseOneTableName + "  where pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);

        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasFieldWithAsTest() {
        String sql = "select varchar_test as xingming ,integer_test  as pid from  " + baseOneTableName + "  where pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasFiledTableTest() {
        String sql =
            "select varchar_test xingming ,integer_test  pid from  " + baseOneTableName + "  as nor where pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);

        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasFiledTableTest1() {
        String sql = "select varchar_test as xingming ,integer_test pid from  " + baseOneTableName + "  nor where pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(ColumnDataGenerator.pkValue);
        selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasTableWithJoinTest() {
        String sql =
            "select n.varchar_test,s.varchar_test studentName,n.pk,s.integer_test from  " + baseOneTableName + "  n , "
                + baseTwoTableName + "  s where n.pk=s.integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select n.varchar_test,s.varchar_test studentName,n.pk,s.integer_test from  " + baseOneTableName
            + "  as n , " + baseTwoTableName
            + "  AS s where n.pk=s.integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasWithFieldJoinTest() {
        String sql = "select  " + baseOneTableName + ".varchar_test name1, " + baseTwoTableName + ".varchar_test, "
            + baseOneTableName + ".pk pk1, " + baseTwoTableName + ".integer_test from  " + baseOneTableName + " , "
            + baseTwoTableName + " where  " + baseOneTableName + ".pk= " + baseTwoTableName + ".integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select  " + baseOneTableName + ".varchar_test as name1, " + baseTwoTableName + ".varchar_test, "
            + baseOneTableName
            + ".pk as pk1, " + baseTwoTableName + ".integer_test from  " + baseOneTableName + " , " + baseTwoTableName
            + " where  " + baseOneTableName + ".pk= " + baseTwoTableName + ".integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasWithFieldTableJoinTest() {
        String sql =
            "select n.varchar_test varchar_test1, " + baseTwoTableName + ".varchar_test,n.pk pk1, " + baseTwoTableName
                + ".integer_test from  "
                + baseOneTableName + "  n, " + baseTwoTableName + " where n.pk= " + baseTwoTableName + ".integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select n.varchar_test as name1, " + baseTwoTableName + ".varchar_test,n.pk pk1, " + baseTwoTableName
            + ".integer_test from  "
            + baseOneTableName + "  as n, " + baseTwoTableName + " where n.pk= " + baseTwoTableName + ".integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore("CoronaDB 不支持 Group By 别名")
    public void aliasWithGroupByTest() {
        String sql = "select count(pk) ,varchar_test as n from  " + baseOneTableName + "   group by n";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasWithOrderByTest() {
        String sql = "select pk as p ,varchar_test as n from  " + baseOneTableName + "   order by p asc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.3.9
     */
    @Test
    public void aliasWithOrderByTest1() {
        String sql = "select pk as p ,varchar_test as n from  " + baseOneTableName + "   order by P";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.3.9
     */
    @Test
    public void aliasWithOrderByTest2() {
        String sql = "select pk as p ,pk as P,varchar_test as n from  " + baseOneTableName + "   order by P";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "select pk as p ,varchar_test as P,varchar_test as n from  " + baseOneTableName + "   order by P";
        selectErrorAssert(sql, null, tddlConnection, "ambiguous");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasWithFuncByTest() {
        String sql = "select count(pk) as con from  " + baseOneTableName + " ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasWithFuncWithGroupBy() {
        String sql = "select integer_test ,sum(pk) as p from " + baseOneTableName + " as a group by integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void aliasWithSubQueryTest() {
        String sql = "select pk,integer_test from (select * from " + baseOneTableName + ") a group by integer_test,pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * 修复 GroupNode + 列别名 导致列无法找到的问题
     */
    @Test
    @Ignore("Not support aggregation functions mixed with DISTINCT and un-distinct.")
    public void aliasWithCountDistinctAndSumTest() {
        String sql =
            "select count(distinct a.`pk`) as count_pk, sum(a.`integer_test`) as sum_integer_test, a.varchar_test as varchar_column from "
                + baseOneTableName + " a group by a.varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * 这个现在还过不了，原因是 GroupNode 和并结果时，COUNT(DISTINCT) 与 其他列的行对应关系错乱
     */
    @Ignore
    public void aliasWithCountDistinctAndSumJoinTest() {
        String sql =
            "select count(distinct a.`integer_test`) as count_pk, sum(a.`integer_test`) as sum_integer_test, a.varchar_test as varchar_column from "
                + baseOneTableName + " a join " + baseTwoTableName
                + " b on a.pk = b.integer_test group by a.varchar_test ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void duplicationColumnsTest() throws Exception {

        String sql = "select count(a.`varchar_test`) from " + baseTwoTableName + " as a left join " + baseTwoTableName
            + " as b on a.pk=b.pk where b.integer_test=10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void duplicationColumnsThreeTablesTest() throws Exception {

        String sql = "select count(a.`varchar_test`) from " + baseOneTableName + " as a left join " + baseTwoTableName
            + " as b on a.pk=b.pk left join " + baseOneTableName
            + " as c on a.varchar_test=c.varchar_test where b.integer_test>10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void duplicationColumnsFourTablesTest() throws Exception {

        String sql = "select count(a.`varchar_test`) from " + baseOneTableName + " as a left join " + baseTwoTableName
            + " as b on a.pk=b.pk left join " + baseOneTableName
            + " as c on a.varchar_test=c.varchar_test join " + baseTwoTableName
            + " as d on a.pk=d.pk and c.integer_test=d.integer_test  where a.varchar_test in('a', 'b') and b.integer_test>10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void duplicationColumnsFourTablesWithSubqueryTest() throws Exception {

        String sql = "select count(a.`varchar_test`) from " + baseOneTableName + " as a left join " + baseTwoTableName
            + " as b on a.pk=b.pk left join " + baseOneTableName
            + " as c on a.varchar_test=c.varchar_test join " + baseTwoTableName
            + " as d on a.pk=d.pk and c.integer_test=d.integer_test  where a.pk in(select pk from "
            + baseTwoTableName + ") and b.integer_test>10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void duplicationColumnsFourTablesWithSubquery2Test() throws Exception {

        String sql = "select count(a.`varchar_test`) from " + baseOneTableName + " as a left join " + baseTwoTableName
            + " as b on a.pk=b.pk left join " + baseOneTableName
            + " as c on a.varchar_test=c.varchar_test join " + baseTwoTableName
            + " as d on a.pk=d.pk and c.integer_test=d.integer_test  where a.pk in (select pk from "
            + baseTwoTableName + " where exists  (select pk from " + baseOneTableName
            + ")) and b.integer_test>10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void aliasBacktick0() {
        String sql = "select 1 as `\" User\".\"createdDate\"`;";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void aliasBacktick1() {
        String sql = "select * from " + baseOneTableName + " `A` where `A`.`pk`=1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void aliasBacktick2() {
        // tinyint_1bit类型 8.0 union length bug，会判断成true、false类型, 目前通过类型推倒解决
        String sql = "select * from (select * from " + baseOneTableName + " union select * from " + baseTwoTableName
            + ") `A` where `A`.`pk`=1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void badAliasTest() {
        final String sql = "SELECT (1)*0.3 (剩)折后金额";
        JdbcUtil.executeQueryFaied(tddlConnection, sql, "Empty method name with args");
    }

}
