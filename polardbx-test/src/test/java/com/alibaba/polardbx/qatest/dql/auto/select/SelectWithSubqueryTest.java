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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.updateErrorAssert;

/**
 * 子查询测试
 *
 * @author mengshi.sunmengshi 2014年4月8日 下午4:50:54
 * @see >,>=,<,<=,=,!=,like all any
 * @since 5.1.0
 */

public class SelectWithSubqueryTest extends AutoReadBaseTestCase {

    private boolean inTrans = false;

    @Parameters(name = "{index}:table0={0},table1={1},inTrans={2}")
    public static List<String[]> prepare() {
        return Arrays.asList(selectBaseOneBaseTwoWithTrans());
    }

    private static String[][] selectBaseOneBaseTwoWithTrans() {

        String[][] object = {
            // case[0]
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

            // trans
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

    public SelectWithSubqueryTest(String baseOneTableName, String baseTwoTableName, String intrans) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
        this.inTrans = Boolean.valueOf(intrans);
    }

    private void selectContentSameAssert(String sql, List<Object> param, Connection mysqlConnection,
                                         Connection tddlConnection, boolean btrue) throws Exception {
        if (inTrans) {
            tddlConnection.setAutoCommit(false);
        }
        DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, btrue);
        if (inTrans) {
            tddlConnection.commit();
            tddlConnection.setAutoCommit(true);
        }
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
     * @since 5.1.0
     */
    @Test
    public void inTest() throws Exception {
        String sql =
            "select * from " + baseOneTableName + " where pk in (select pk from " + baseTwoTableName + " where pk<100)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    @Ignore
    public void columnPushTest() throws Exception {
        String sql = "select a.* , (select count(1) from " + baseOneTableName + " where pk = 1) as COUNT from "
            + baseOneTableName + " a where pk = 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void inPushTest() throws Exception {
        String sql =
            "select * from " + baseOneTableName + " where varchar_test in (select varchar_test from " + baseTwoTableName
                + " where integer_test < 100) and pk >100";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void joinFilterinPushTest() throws Exception {
        String sql = "select n.pk,n.varchar_test from " + baseOneTableName + " n , " + baseTwoTableName
            + " s where n.pk = s.pk and n.varchar_test in (select varchar_test from " + baseTwoTableName
            + " where integer_test < 100) and n.pk >100";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void joinFilterinTest() throws Exception {
        String sql = "select n.pk,n.varchar_test from " + baseOneTableName + " n , " + baseTwoTableName
            + " s where n.pk = s.pk and n.varchar_test in (select varchar_test from " + baseTwoTableName
            + " where integer_test in (1,2,3,4)) and n.pk >100";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void wherePushTest() throws Exception {
        String sql =
            "select * from " + baseOneTableName + " where varchar_test = (select varchar_test from " + baseTwoTableName
                + " where pk = 1) and pk = 1 and integer_test = 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void equalTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk = (select pk from " + baseTwoTableName
            + " order by pk limit 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void equalMaxTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk = (select max(pk) from " + baseTwoTableName
            + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void greaterTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk > (select pk from " + baseTwoTableName
            + " order by pk asc limit 5,1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void greaterEqualTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk >= (select pk from " + baseTwoTableName
            + " order by pk asc limit 5,1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void lessTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk < (select pk from " + baseTwoTableName
            + " order by pk asc limit 5,1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void lessEqualTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk <= (select pk from " + baseTwoTableName
            + " order by pk asc limit 5,1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void notEqualTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk != (select pk from " + baseTwoTableName
            + " order by pk asc limit 5,1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void likeTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where varchar_test like (select varchar_test from "
            + baseTwoTableName
            + " order by pk asc limit 5,1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void greaterAnyTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk > any(select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void greaterEqualAnyTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk >= any(select pk from " + baseTwoTableName
            + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void lessAnyTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk < any(select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void lessEqualAnyTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk <= any(select pk from " + baseTwoTableName
            + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void notEqualAnyTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk != any(select pk from " + baseTwoTableName
            + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void notEqualAnyOneValueTest() throws Exception {
        String sql = "select * from " + baseOneTableName + " where pk != any(select pk from " + baseTwoTableName
            + " where pk=1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void equalAnyTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk = any(select pk from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void equalAnyMaxTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk = any(select max(pk) from " + baseTwoTableName
            + " group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void greatAnyMaxTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk > any(select max(pk) from " + baseTwoTableName
            + " group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void greatEqualAnyMaxTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk >= any(select max(pk) from " + baseTwoTableName
            + " group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void lessAnyMaxTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk < any(select max(pk) from " + baseTwoTableName
            + " group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void lessEqualAnyMaxTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk <= any(select max(pk) from " + baseTwoTableName
            + " group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void notEqualAnyMaxSomeValueTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk != any(select max(pk) from " + baseTwoTableName
            + " group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support any")
    public void notEqualAnyMaxOneValueTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk != any(select max(pk) from " + baseTwoTableName
            + " group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support all")
    public void equalAllMaxTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk = all(select max(pk) from " + baseTwoTableName
            + " group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support all")
    public void equalAllMaxOneValueTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk = all(select max(pk) from " + baseTwoTableName
            + " )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support all")
    public void greatAllMaxTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk > all(select max(pk) from " + baseTwoTableName
            + " where pk<100 group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support all")
    public void greatEqualAllMaxTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk >= all(select max(pk) from " + baseTwoTableName
            + " group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support all")
    public void lessAllMaxTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk < all(select max(pk) from " + baseTwoTableName
            + " where pk =100  group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support all")
    public void lessEqualAllMaxTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk <= all(select max(pk) from " + baseTwoTableName
            + " group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support all")
    public void notEqualAllMaxTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk != all(select max(pk) from " + baseTwoTableName
            + " where pk<100  group by pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support all")
    public void equalAllOneValueTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk = all(select pk from " + baseTwoTableName
            + " where pk=1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Ignore("does not support all")
    public void equalAllSomeValueTest() throws Exception {

        String sql = "select * from " + baseOneTableName + " where pk = all(select pk from " + baseTwoTableName
            + " where pk=10)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     *
     */
    @Test
    public void correlativeSubQueryTest() throws Exception {

        // 添加测试数据
        List<Map<String, String>> idAndDataList1 = new ArrayList<Map<String, String>>();
        List<Map<String, String>> idAndDataList2 = new ArrayList<Map<String, String>>();

        Map<String, String> idAndData1 = new HashMap<String, String>();
        idAndData1.put("id", "1100");
        idAndData1.put("data", "abc");
        idAndDataList1.add(idAndData1);

        Map<String, String> idAndData2 = new HashMap<String, String>();
        idAndData2.put("id", "1104");
        idAndData2.put("data", "efg");
        idAndDataList1.add(idAndData2);

        Map<String, String> idAndData3 = new HashMap<String, String>();
        idAndData3.put("id", "1110");
        idAndData3.put("data", "NULL");
        idAndDataList1.add(idAndData3);
        idAndDataList2.add(idAndData3);

        Map<String, String> idAndData4 = new HashMap<String, String>();
        idAndData4.put("id", "1111");
        idAndData4.put("data", "hello");
        idAndDataList1.add(idAndData4);
        idAndDataList2.add(idAndData4);

//        prepareForPreTblAndPostTbl("pre_tbl", idAndDataList1);
//        prepareForPreTblAndPostTbl("post_tbl", idAndDataList2);
//
//        // 校验
//        String testSql = String.format("select id, note from pre_tbl where not exists"
//        		+ " ( select * from post_tbl where pre_tbl.id=post_tbl.id and post_tbl.msg=pre_tbl.note )");
//
//        String[] columnParam = { "id", "note" };
//        selectContentSameAssert(testSql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void selectConstantInSubQueryTest() throws Exception {
        List<String> sqls = new ArrayList<>();

        sqls.add(
            String.format("select * from %s where exists (select '' from %s)", baseOneTableName, baseTwoTableName));
        sqls.add(
            String.format("select * from %s where exists (select ' ' from %s)", baseOneTableName, baseTwoTableName));
        sqls.add(
            String.format("select * from %s where exists (select '*' from %s)", baseOneTableName, baseTwoTableName));
        sqls.add(
            String.format("select * from %s where exists (select '?' from %s)", baseOneTableName, baseTwoTableName));
        sqls.add(
            String.format("select * from %s where exists (select '1' from %s)", baseOneTableName, baseTwoTableName));
        sqls.add(String.format("select * from %s where exists (select 1 from %s)", baseOneTableName, baseTwoTableName));

        for (String sql : sqls) {
//            System.out.println("0: " + sql);
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
//            System.out.println("1: " + sql);
        }
    }

    /**
     * 和 selectConstantInSubQueryTest 类似，但 subquery 里的表和外边的表为同一个，目前这个 sql
     * 在生成执行计划时会出错，先 ignore
     */
    @Ignore
    @Test
    public void selectConstantInSubQueryTest2() throws Exception {
        List<String> sqls = new ArrayList<>();

        sqls.add(
            String.format("select * from %s where exists (select '' from %s)", baseOneTableName, baseOneTableName));
        sqls.add(
            String.format("select * from %s where exists (select ' ' from %s)", baseOneTableName, baseOneTableName));
        sqls.add(
            String.format("select * from %s where exists (select '*' from %s)", baseOneTableName, baseOneTableName));
        sqls.add(
            String.format("select * from %s where exists (select '?' from %s)", baseOneTableName, baseOneTableName));
        sqls.add(
            String.format("select * from %s where exists (select '1' from %s)", baseOneTableName, baseOneTableName));
        sqls.add(String.format("select * from %s where exists (select 1 from %s)", baseOneTableName, baseOneTableName));

        for (String sql : sqls) {
//            System.out.println("0: " + sql);
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
//            System.out.println("1: " + sql);
        }
    }

    /**
     * 当子查询表别名相同且无查询字段，是否能递归向上寻找
     * 在生成执行计划时会出错，先 ignore
     */
    @Test
    public void selectAliasSubQueryTest() throws Exception {
        List<String> sqls = new ArrayList<>();
        sqls.add(String.format("select b.pk from " + baseOneTableName
            + " b where b.varchar_test in (select b.char_test from (select pk from " + baseOneTableName
            + " order by pk asc limit 1) b) order by pk asc limit 5", baseOneTableName, baseOneTableName));

        for (String sql : sqls) {
//            System.out.println("0: " + sql);
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
//            System.out.println("1: " + sql);
        }
    }

    @Test
    public void testProjectSubqueryWithLimit() throws Exception {
        String sql = "select t1.pk from " + baseOneTableName + " t1 join " + baseTwoTableName
            + " t2 on  t1.integer_test= t2.integer_test order by t1.pk limit 5";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
        sql = "  select pk,integer_test,(select integer_test from " + baseTwoTableName
            + " where a.integer_test = integer_test limit 1)  from (select t1.* from " + baseOneTableName + " t1 join "
            + baseTwoTableName + " t2 on  t1.integer_test= t2.integer_test order by t1.pk limit 5) a ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);

//        sql="select * from " + baseTwoTableName;
//        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
    }

    @Ignore
    public void testAggProjectMergeRule() throws Exception {
        String sql = " select pk,integer_test,(select integer_test from " + baseTwoTableName
            + " where t1.integer_test = integer_test limit 1)  from " + baseOneTableName
            + " t1 group by t1.pk,integer_test ";
        selectContentSameAssert(sql, null, tddlConnection, mysqlConnection, false);

        //        sql="select * from " + baseTwoTableName;
        //        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
    }

    @Test
    public void testProjectSubquery2couldbepush() throws Exception {

        String sql = "  select pk,integer_test,(select sum(integer_test) from " + baseTwoTableName
            + " where a.varchar_test = varchar_test ) ,(select sum(integer_test) from " + baseOneTableName
            + " where a.varchar_test=varchar_test) from  " + baseTwoTableName + " a";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);

        //        sql="select * from " + baseTwoTableName;
        //        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
    }

    @Test
    public void testProjectJASubquery() throws Exception {

        String sql = "  select pk,integer_test,(select count(integer_test) from " + baseTwoTableName
            + " where a.varchar_test = varchar_test )  from  " + baseTwoTableName + " a";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);

    }

    @Test
    public void testProjectJASubquery2() throws Exception {

        String sql = "  select pk,integer_test,(select count(integer_test) from " + baseTwoTableName
            + " )  from  " + baseTwoTableName + " a";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);

    }

    @Test
    public void testProjectSubquery() throws Exception {
        String sql = "  select pk,integer_test,(select integer_test from " + baseTwoTableName
            + " where a.pk=pk)  from  " + baseOneTableName + " a";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
    }

    @Test
    public void testCorrelatedSubqueryApplyWithLimit() throws Exception {
        String sql = String.format("select pk from %s where pk in "
                + "(select a.pk from %s a JOIN %s b "
                + "ON a.pk=b.pk order by a.`tinyint_1bit_test`,a.`pk`) order by pk limit 10, 30;", baseOneTableName,
            baseTwoTableName, baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
    }

    /**
     *
     */
    @Test
    public void testColumnNotFoundSubquery() throws SQLException {
        if (!baseOneTableName.endsWith(ExecuteTableName.ONE_DB_ONE_TB_SUFFIX)) {
            return;
        }

        String sql = "select pk,integer_test from " + baseOneTableName
            + " n where n.pk in (select m.id from " + baseOneTableName + " m);";
        final Statement statement = tddlConnection.createStatement();
        try {
            statement.execute(sql);
        } catch (Exception e) {
            final String message = e.getMessage();
            Assert.assertTrue(!message.contains(String.format("Table '%s' not found.", "m")),
                "expect 'Column not found' but got 'Table not found'");
            Assert.assertTrue(message.contains(String.format("Column '%s' not found", "id")),
                "expect 'Column not found'.");
        }

    }

    @Test
    public void testSubqueryNotSupportInGroupByOrderByLimit() throws SQLException {
        String sql = "select pk,integer_test from " + baseOneTableName
            + " n group by (select m.id from " + baseOneTableName + " m);";
        selectErrorAssert(sql, null, tddlConnection, "subuqery in group by/order by/limit  not support yet");

        sql = "select pk,integer_test from " + baseOneTableName
            + " n order by (select m.id from " + baseOneTableName + " m) is null;";
        selectErrorAssert(sql, null, tddlConnection, "subuqery in group by/order by/limit  not support yet");

        sql = "select pk,integer_test from " + baseOneTableName
            + " n limit (select m.id from " + baseOneTableName + " m);";
        selectErrorAssert(sql, null, tddlConnection, "subuqery in group by/order by/limit  not support yet");
    }

    @Test
    public void testProjectASubquery() throws Exception {

        String sql = "  select pk,integer_test,(select count(integer_test) from " + baseTwoTableName
            + " where a.varchar_test = varchar_test )  from  " + baseTwoTableName + " a";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);

    }

    /**
     * test ERR_SUBQUERY_VALUE_NOT_READY error
     */
    static boolean testSubqueryWithGsi = false;

    @Test
    public void testSubqueryWithGsi() {
        if (testSubqueryWithGsi) {
            return;
        }
        testSubqueryWithGsi = true;
        // prepare catalog
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists testSubqueryWithGsi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists testSubqueryWithGsi2");
        String createTbl1 =
            "CREATE TABLE if not exists `testSubqueryWithGsi1` ( `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, `customer_id` bigint(20) DEFAULT NULL COMMENT '租户id', `shipper` varchar(64) DEFAULT NULL COMMENT '托运人code', PRIMARY KEY USING BTREE (`id`), INDEX `customer_shipper` USING BTREE (`customer_id`, `shipper`)) ENGINE = InnoDB DEFAULT CHARSET = utf8";
        String createTbl2 =
            "CREATE TABLE if not exists `testSubqueryWithGsi2` ( `customer_id` bigint(20) NOT NULL , PRIMARY KEY USING BTREE (`customer_id`)) ENGINE = InnoDB DEFAULT CHARSET = utf8";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTbl1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTbl2);

        String sql = "SELECT 1\n"
            + "FROM testSubqueryWithGsi1\n"
            + "WHERE customer_id = 1000\n"
            + "  AND (customer_id IN\n"
            + "         (SELECT customer_id\n"
            + "          FROM testSubqueryWithGsi2)\n"
            + "       OR customer_id NOT IN\n"
            + "         (SELECT customer_id\n"
            + "          FROM testSubqueryWithGsi2));";
        JdbcUtil.executeQuery(sql, tddlConnection);

        // clear
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists testSubqueryWithGsi1");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists testSubqueryWithGsi2");
    }

}
