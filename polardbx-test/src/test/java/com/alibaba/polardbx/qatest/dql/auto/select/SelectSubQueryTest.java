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
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConfigUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.polardbx.qatest.validator.DataValidator.assertShardCount;
import static com.alibaba.polardbx.qatest.validator.DataValidator.explainAllResultMatchAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectStringContentIgnoreCaseSameAssert;

/**
 * 子查询
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class SelectSubQueryTest extends AutoReadBaseTestCase {

    private static AtomicBoolean shardingAdvise = new AtomicBoolean(false);

    long pk = 12l;
    ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwoBaseThree());
    }

    public SelectSubQueryTest(String baseOneTableName, String baseTwoTableName, String baseThreeTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
        this.baseThreeTableName = baseThreeTableName;

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void comparisonTest() {
        String sql =
            "select pk,varchar_test,integer_test  from " + baseOneTableName + " where integer_test =(select pk from "
                + baseTwoTableName
                + " where varchar_test='" + columnDataGenerator.varchar_testValue + "'  order by pk  limit 1 )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql =
            "select pk,varchar_test,integer_test    from " + baseOneTableName + " where integer_test <(select pk from "
                + baseTwoTableName
                + " where varchar_test='" + columnDataGenerator.varchar_testValue + "' order by pk limit 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        sql =
            "select  pk,varchar_test,integer_test   from " + baseOneTableName + " where integer_test <=(select pk from "
                + baseTwoTableName
                + " where varchar_test='" + columnDataGenerator.varchar_testValue + "'  order by pk limit 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql =
            "select  pk,varchar_test,integer_test    from " + baseOneTableName + " where integer_test >(select pk from "
                + baseTwoTableName
                + " where varchar_test='" + columnDataGenerator.varchar_testValue + "'  order by pk limit 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select *  from " + baseOneTableName + " where integer_test >=(select pk from " + baseTwoTableName
            + " where varchar_test='" + columnDataGenerator.varchar_testValue + "'  order by pk limit 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void comparisonSubWithOrderByLimitTest() {
        String sql =
            "select  pk,varchar_test,integer_test   from " + baseOneTableName + " where integer_test =(select pk from "
                + baseTwoTableName
                + " order by pk limit 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select *  from " + baseOneTableName + " where integer_test >(select pk from " + baseTwoTableName
            + " order by pk  limit 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select *  from " + baseOneTableName + " where integer_test >(select pk from " + baseTwoTableName
            + " order by pk asc limit 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select *  from " + baseOneTableName + " where integer_test <(select pk from " + baseTwoTableName
            + " order by pk desc limit 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select *  from " + baseOneTableName + " where integer_test >(select pk from " + baseTwoTableName
            + " where varchar_test like '" + columnDataGenerator.varchar_testValue + "' order by  pk limit 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void comparisonWithOrderByLimitTest() {
        String sql =
            "select pk,varchar_test,integer_test  from " + baseOneTableName + " where integer_test >(select pk from "
                + baseTwoTableName
                + " where varchar_test like '" + columnDataGenerator.varchar_tesLikeValueOne
                + "' order by  pk limit 1) order by pk";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select pk,varchar_test,integer_test   from " + baseOneTableName + " where integer_test >(select pk from "
            + baseTwoTableName
            + " where varchar_test like '" + columnDataGenerator.varchar_tesLikeValueOne
            + "' order by  pk limit 1) order by pk limit 2";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select count(pk), varchar_test from "
            + baseOneTableName
            + " where integer_test >(select pk from "
            + baseTwoTableName
            + " where varchar_test like '" + columnDataGenerator.varchar_tesLikeValueOne
            + "' order by  pk limit 1) group by  varchar_test order by count(pk) ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        //TODO select varchar_test from select_join_first_mutil_db_mutil_tb where varchar_test like '%e%' group by varchar_test order by pk limit 1  返回结果错误
//        /* ANDOR ALLOW_TEMPORARY_TABLE=True */select pk ,(select varchar_test from select_join_first_mutil_db_mutil_tb where varchar_test like '%e%' group by varchar_test order by pk limit 1 )as name from select_base_one_db_mutil_tb 
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void comparisonWithFuncTest() {
        String sql = "select  pk,varchar_test,integer_test    from " + baseOneTableName
            + " where pk <(select max(integer_test) from " + baseTwoTableName
            + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " where integer_test <=(select max(pk) from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select pk,varchar_test,integer_test   from " + baseOneTableName
            + " where integer_test =(select min(pk) from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select  pk,varchar_test,integer_test    from " + baseOneTableName
            + " where integer_test <(select count(*) from " + baseTwoTableName + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-1
     */
    @Test
    @Ignore("Group_concat")
    public void comparisonWithGroupConcatTest() {
        String sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " where integer_test =(select group_concat(pk) from "
            + baseTwoTableName + " where pk = 10) ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " where integer_test <=(select group_concat(pk) from "
            + baseTwoTableName + " where pk = 10)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " where integer_test =(select group_concat(pk) from "
            + baseTwoTableName + " where pk = 10)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-1
     */
    @Test
    @Ignore("Group_concat")
    public void comparisonWithGroupConcatTest2() {
        String sql = "select *  from " + baseOneTableName + " where integer_test =(select group_concat(pk) from "
            + baseTwoTableName + " where pk = 10) ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select *  from " + baseOneTableName + " where integer_test <=(select group_concat(pk) from "
            + baseTwoTableName + " where pk = 10)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select *  from " + baseOneTableName + " where integer_test =(select group_concat(pk) from "
            + baseTwoTableName + " where pk = 10)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void anyTest() {
        String sql =
            "select   pk,varchar_test,integer_test    from " + baseOneTableName + " where pk =any(select pk from "
                + baseTwoTableName
                + " where pk>" + pk + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName + " where pk <any(select pk from "
            + baseTwoTableName
            + " where pk>" + pk + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName + " where pk >any(select pk from "
            + baseTwoTableName
            + " where pk>" + pk + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName + " where pk <>any(select pk from "
            + baseTwoTableName
            + " where pk>" + pk + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void allTest() {
        String sql = "select  pk,varchar_test,integer_test  from " + baseOneTableName + " where pk =ALL(select pk from "
            + baseTwoTableName
            + " where pk>" + pk + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName + " where pk <ALL(select pk from "
            + baseTwoTableName
            + " where pk>" + pk + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);

        sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName + " where pk >ALL(select pk from "
            + baseTwoTableName
            + " where pk<" + pk + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName + " where pk <>ALL(select pk from "
            + baseTwoTableName
            + " where pk>" + pk + ")";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void existsTest() {

        String sql =
            "select   pk,varchar_test,integer_test   from " + baseOneTableName + " where EXISTS (select * from "
                + baseTwoTableName + " where " + baseOneTableName
                + ".integer_test=" + baseTwoTableName + ".pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        /** unsupported
         sql = "select   pk,varchar_test,integer_test   from " + baseOneTableName + " where not EXISTS (select * from " + baseTwoTableName + " where " + baseOneTableName
         + ".integer_test>" + baseTwoTableName + ".pk)";
         //分表bug已经经修复，放开用户去掉if
         selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
         */

        /** unsupported
         sql = "select   pk,varchar_test,integer_test   from " + baseOneTableName + " where not EXISTS (select * from " + baseTwoTableName + " where " + baseOneTableName
         + ".integer_test>" + baseOneTableName + ".pk)";
         selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
         */

        sql = "select    pk,varchar_test,integer_test   from " + baseOneTableName + " where EXISTS (select * from "
            + baseTwoTableName + " where " + baseTwoTableName
            + ".integer_test=" + baseOneTableName + ".pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "select   pk,varchar_test,integer_test   from " + baseOneTableName + " where EXISTS (select * from "
            + baseTwoTableName + " where " + baseOneTableName
            + ".integer_test=" + baseTwoTableName + ".pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void existsOrderbyTest() {
        String sql = String.format(
            "SELECT this_.integer_test as idx  FROM %s this_ WHERE EXISTS (SELECT 1 FROM %s WHERE this_.integer_test=pk) "
                + "ORDER BY this_.integer_test DESC LIMIT 5",
            baseOneTableName,
            baseTwoTableName);

        // String sql = "select  * from " + baseOneTableName +
        // " where EXISTS (select * from " + baseTwoTableName + " where "
        // + baseOneTableName + ".hostgroup_id=" + baseTwoTableName + ".hostgroup_id)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void associationTest() {
        String sql = "select   pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where pk in (1,2,3,4,5)  and (select count(*) from " + baseTwoTableName
            + " as info where info.pk=host.pk and host.integer_test > info.integer_test) < 2 order by pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void selfCorrelatedSubQuery() {
        String sql = "select   pk,varchar_test,mediumint_test   from "
            + baseOneTableName
            + " as host where pk in (1,2,3,5) and (select count(*) from "
            + baseOneTableName
            + " as info where info.pk = host.pk and info.mediumint_test < host.mediumint_test) < 2 order by pk limit 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * limit test
     */
    @Test
    public void testFilterScalarSubqueryWithOrderbyTest() {
        if (baseOneTableName.contains("one_db_one_tb") && baseTwoTableName.contains("one_db_one_tb")) {
            return;
        }
        String sql = "explain select   pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where pk = (select pk from " + baseTwoTableName
            + " as info where info.integer_test=host.integer_test order by pk limit 1)";
        explainAllResultMatchAssert(sql, null, tddlConnection,
            "[\\s\\S]*" + "CorrelateApply" + "[\\s\\S]*");
    }

    @Test
    public void testInByConstants() {
        String sql = "select * from (select varchar_test, pk as id from " + baseOneTableName
            + " b) a where id in (select 1 from " + baseTwoTableName + " c)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * limit test
     */
    @Test
    public void testFilterInSubqueryWithOrderbyTest() {
        if (baseOneTableName.contains("one_db_one_tb") && baseTwoTableName.contains("one_db_one_tb")) {
            return;
        }
        String sql = "explain select   pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where pk in (select pk from " + baseTwoTableName
            + " as info where info.integer_test=host.integer_test order by pk limit 1)";
        explainAllResultMatchAssert(sql, null, tddlConnection,
            "[\\s\\S]*" + "CorrelateApply" + "[\\s\\S]*");
    }

    /**
     * limit test
     */
    @Test
    public void testProjectScalarSubqueryWithOrderbyTest() {
        if (baseOneTableName.contains("one_db_one_tb") && baseTwoTableName.contains("one_db_one_tb")) {
            return;
        }
        String sql = "explain select pk,varchar_test, pk = (select pk from " + baseTwoTableName
            + " as info where info.integer_test=host.integer_test order by pk limit 1) from " + baseOneTableName
            + " as host ";
        explainAllResultMatchAssert(sql, null, tddlConnection,
            "[\\s\\S]*" + "CorrelateApply" + "[\\s\\S]*");
    }

    /**
     * limit test
     */
    @Test
    public void testProjectInSubqueryWithOrderbyTest() {
        if (baseOneTableName.contains("one_db_one_tb") && baseTwoTableName.contains("one_db_one_tb")) {
            return;
        }
        String sql = "explain select pk,varchar_test, pk in (select pk from " + baseTwoTableName
            + " as info where info.integer_test=host.integer_test order by pk limit 1) from " + baseOneTableName
            + " as host ";
        explainAllResultMatchAssert(sql, null, tddlConnection,
            "[\\s\\S]*" + "CorrelateApply" + "[\\s\\S]*");
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void associationTestIn() {
        String sql = "select   pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where host.pk in (select pk from "
            + baseTwoTableName + " as info where host.integer_test+100=info.pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void associationTestNotIn() {
        String sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where host.pk not in (select pk from "
            + baseTwoTableName + " as info where info.pk=host.integer_test)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * @since 5.0.1
     */
    @Test
    public void associationExistsTest() {
        String sql =
            "select  pk,varchar_test,integer_test   from " + baseOneTableName + " as host where exists (select pk from "
                + baseTwoTableName
                + " as info where host.integer_test=info.pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void associationExistsAndSubqueryTest() {
        String sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where exists ( select * from (select pk from "
            + baseTwoTableName + " as info) w where host.integer_test=w.pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void associationExistsAndSubquery_push_Test() {
        String sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where host.pk <10 and exists ( select * from (select integer_test from "
            + baseTwoTableName + " as info where info.pk < 100) w where host.integer_test=w.integer_test)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void associationExistsAndSubquery_multi_push_Test() {
        String sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where host.pk< 10 and host.integer_test in ( select integer_test from "
            + baseTwoTableName + " info where info.pk <100 )"
            + " and exists ( select * from (select integer_test from " + baseTwoTableName
            + " as info where info.pk <100) w where host.integer_test=w.integer_test)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void associationExistsAndSubquery_multi_push_failed1_Test() {
        String sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where host.pk  >10 and host.integer_test in ( select integer_test from "
            + baseTwoTableName + " info where info.pk  >20 )"
            + " and exists ( select * from (select integer_test from " + baseTwoTableName
            + " as info where info.pk = 10) w where host.integer_test=w.integer_test)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void associationExistsAndSubquery_multi_push_failed2_Test() {
        String sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where host.pk >100 and host.integer_test in ( select integer_test from "
            + baseTwoTableName + " info where info.pk  <100 )"
            + " and exists ( select * from (select integer_test from " + baseTwoTableName
            + " as info where info.pk = 2) w where host.integer_test=w.integer_test)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void associationExistsAndSubquery_multi_push_failed12_Test() {
        String sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where host.pk = 1 and host.integer_test in ( select integer_test from "
            + baseTwoTableName + " info where info.pk < 200 )"
            + " and exists ( select * from (select integer_test from " + baseTwoTableName
            + " as info where info.pk < 300) w where host.integer_test=w.integer_test)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void associationNotExistsTest() {
        String sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where not exists (select pk from " + baseTwoTableName
            + " as info where host.integer_test>info.pk)";
        //分表情况有bug，暂时忽略，后续修复再打开
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void associationExistsTest2() {
        String sql =
            "select  pk,varchar_test,integer_test   from " + baseOneTableName + " as host where exists (select pk from "
                + baseTwoTableName
                + " as info where host.integer_test=info.pk and exists(select pk from "
                + baseTwoTableName + " as info2 where info2.pk=info.pk))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void associationNotExistsTest2() {
        String sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where not exists (select pk from " + baseTwoTableName
            + " as info where host.integer_test=info.pk and not exists(select pk from "
            + baseTwoTableName + " as info2 where info2.pk=info.pk))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void associationExistsTest3() {
        String sql =
            "select  pk,varchar_test,integer_test   from " + baseOneTableName + " as host where exists (select pk from "
                + baseTwoTableName
                + " as info where host.integer_test=info.pk and exists(select pk from "
                + baseTwoTableName
                + " as info2 where info2.pk=info.pk and info.pk in (select pk from "
                + baseTwoTableName + " as info3 where info2.pk=info3.pk)))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore
    public void associationNotExistsTest3() {
        String sql = "select  pk,varchar_test,integer_test   from " + baseOneTableName
            + " as host where not exists (select pk from " + baseTwoTableName
            + " as info where host.integer_test=info.pk and not exists(select pk from "
            + baseTwoTableName
            + " as info2 where info2.pk=info.pk and info.pk in (select pk from "
            + baseTwoTableName + " as info3 where info2.pk=info3.pk)))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void fromTest() {
        String sql = "select host,varchar_test from (select pk*2 as host ,varchar_test from " + baseOneTableName + ""
            + " )as sb where host>" + pk;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select avg(sumHost) from (select sum(pk) as sumHost from " + baseOneTableName + ""
            + " group by pk )as sb";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void fromNestTest() {
        String sql = "select host,varchar_test from (select pk,pk*2 as host ,varchar_test from " + baseOneTableName
            + " f where f.pk>0 and exists ( select * from " + baseOneTableName
            + " n where n.pk = f.pk )" + " )as sb where host>" + pk;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        // sql =
        // "select avg(sumHost) from (select host_id,sum(host_id) as sumHost from "
        // + baseOneTableName
        // + " f where f.host_id>0 and exists (select * from " + baseOneTableName +
        // " n where n.host_id = f.host_id )"
        // + " group by host_id )as sb";
        // String[] columnParam1 = { "avg(sumHost)" };
        // selectContentSameAssert(sql, columnParam1, null);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void CloumnTest() {
        String sql = "select pk ,(select varchar_test from " + baseTwoTableName + " where pk =" + pk
            + ")as name from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select pk ,(select varchar_test from " + baseTwoTableName
            + " where varchar_test like '" + columnDataGenerator.varchar_tesLikeValueOne
            + "' group by varchar_test order by varchar_test limit 1"
            + " )as name from " + baseOneTableName;
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void SubqueryNestTest() {
        String sql = "select pk,varchar_test from " + baseOneTableName + " a where a.pk in (select pk from "
            + baseOneTableName + " b where b.pk in  ( select pk from " + baseOneTableName
            + " c where c.pk > 0 ))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void SubqueryNest1Test() throws SQLException {
        String sql = "SELECT *\n"
            + "FROM tbl1\n"
            + "WHERE a>\n"
            + "    SELECT date_format\n"
            + "       (SELECT IFNULL(USE_TIME, curdate())\n"
            + "        FROM tbl\n"
            + "        WHERE tbl1.b=b,\n"
            + "            '%Y-%m-%d %H:%i:%s')";
        // build schema
        tddlConnection.createStatement().execute("CREATE DATABASE IF NOT EXISTS DRDS_SUBQUERY_TEST_DB mode='drds'");
        tddlConnection.createStatement().execute("USE DRDS_SUBQUERY_TEST_DB");
        // build tbl
        tddlConnection.createStatement().execute("CREATE TABLE if not exists `tbl1` (\n"
            + "\t`a` int(11) NOT NULL,\n"
            + "\t`b` int(11) DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`a`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4");
        tddlConnection.createStatement().execute("CREATE TABLE if not exists `tbl` (\n"
            + "\t`a` int(11) NOT NULL,\n"
            + "\t`b` int(11) DEFAULT NULL,\n"
            + "\t`USE_TIME` datetime DEFAULT NULL,\n"
            + "\tPRIMARY KEY (`a`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`a`)");

        // prepare data
        tddlConnection.createStatement().execute("delete from tbl");
        tddlConnection.createStatement().execute("delete from tbl1");
        tddlConnection.createStatement().execute("insert into tbl values(3,2,null)");
        tddlConnection.createStatement().execute("insert into tbl values(1,2,now())");
        tddlConnection.createStatement().execute("insert into tbl1 values(1,3)");
        tddlConnection.createStatement().execute("insert into tbl1 values(12,11)");
        tddlConnection.createStatement().executeQuery(sql);
    }

    /**
     * @since 5.2.7
     */
//    @Ignore("暂时未修复的bug")
    @Test
    public void fromNestTestConsant() {
        String sql = "select * from (select 1 from dual)x ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select 1 from (select 1 from dual)x ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "SELECT * FROM (SELECT 1 from dual UNION ALL SELECT 1 from dual ) x ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testSubqueryIteration() {

        /**
         * 测试 Semi Anti JOIN 执行器对 current() 函数的处理。
         */
        String sql =
            "select  pk,\n" + "       varchar_test,\n" + "       integer_test FROM " + baseOneTableName + " AS HOST\n"
                + "WHERE EXISTS\n" + "    (SELECT pk\n" + "     FROM " + baseTwoTableName + " AS info\n"
                + "     WHERE HOST.integer_test=info.pk\n" + "       AND EXISTS\n" + "         (SELECT pk\n"
                + "          FROM " + baseTwoTableName + " AS info2\n"
                + "          WHERE info2.pk=info.pk));";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    @Test
    public void testSubqueryTypeMisMatch() {
        /**
         * 测试 filterHandler 中处理非关联子查询的代码
         * 为防止 filter 被推入到 LV 中，构建了一个不可下推的 JOIN
         * 为防止子查询在 filter cursor 中处理，生成的非关联子查询。
         */
        String sql =
            "SELECT * \n" + "FROM (select a1.pk,a1.integer_test from " + baseOneTableName + " a1, " + baseOneTableName
                + " a2 where a1.pk=a2.integer_test) T1 \n" + "WHERE  integer_test <=\n"
                + "    (SELECT pk\n" + "     FROM " + baseTwoTableName + "\n"
                + " order by pk  limit 1 \n" + " ) order by pk limit 10";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

    }

    /**
     * user feedback cases
     */

    /**
     * uion (join with correlate subquery in filter)s
     */
    @Test
    public void testSubqueryJoinWithCorrelateSub() {

        String sql = "SELECT * \n" + "FROM (select a1.pk,a1.integer_test from " + baseOneTableName + " a1 join "
            + baseOneTableName + " a2 on a1.pk=a2.integer_test  \n" + "WHERE  not exists \n"
            + "    (SELECT pk\n" + "     FROM " + baseTwoTableName + "\n"
            + " where pk = a1.pk \n" + " ) union select  a4.pk,a4.integer_test from " + baseOneTableName + " a4 join "
            + baseOneTableName + " a3 on a4.pk=a3.integer_test   ) t";
        selectStringContentIgnoreCaseSameAssert(sql, null, mysqlConnection, tddlConnection, true, true);

    }

    /**
     * test scalar subquery in filter
     */
    @Test
    public void semiJoinWithInnerTypeExecutorTest() {
        String sql =
            "select pk,varchar_test,integer_test  from " + baseOneTableName + " where integer_test =(select pk from "
                + baseTwoTableName
                + " where pk=56  limit 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
        sql = "select pk,varchar_test,integer_test  from " + baseOneTableName + " where integer_test >(select pk from "
            + baseTwoTableName
            + " where pk=56  limit 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);

    }

    @Test
    public void semiJoinPushDownLeftToRightValuepassInTest() {
        String sql =
            "select pk,varchar_test,integer_test  from " + baseOneTableName + " where pk=56 and pk  in (select pk from "
                + baseTwoTableName
                + "   )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
    }

    @Test
    public void semiJoinPushDownLeftToRightValuepassExistsTest() {
        String sql = "select pk,varchar_test,integer_test  from " + baseOneTableName
            + " t1 where pk=56 and exists (select pk from " + baseTwoTableName
            + "   where t1.pk=pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
    }

    @Test
    public void semiJoinPushDownRightToLeftValuepassInTest() {
        String sql = "select pk,varchar_test,integer_test  from " + baseOneTableName + " where pk  in (select pk from "
            + baseTwoTableName
            + "   where pk=56 )";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
    }

    @Test
    public void semiJoinPushDownRightToLeftValuepassExistsTest() {
        String sql =
            "select pk,varchar_test,integer_test  from " + baseOneTableName + " t1 where exists (select pk from "
                + baseTwoTableName
                + "   where t1.pk=pk and pk=56)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void inPushTest1() throws Exception {
        String sql = MessageFormat.format(
            "select * from {0} a where pk in (select pk from {1} b where pk = 56)",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void inPushTest2() throws Exception {
        String sql = MessageFormat.format(
            "select * from (select varchar_test, pk as id from {0} a) a where id in (select pk from {1} b where pk = 56)",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void inPushTest3() throws Exception {
        String sql = MessageFormat
            .format("select * from (select varchar_test, pk as id from {0} a) a where "
                    + "id in (select pk from {1} b) and id = 56 and exists(select * from {2} c where a.id = pk)",
                baseOneTableName,
                baseTwoTableName,
                baseThreeTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void inPushTest4() throws Exception {
        String sql = MessageFormat.format("select * from (select varchar_test, pk as id from {0} a) a where "
                + "id in (select pk from {1} b where pk = 56 ) "
                + "and exists(select * from {2} c where a.id = c.pk)",
            baseOneTableName,
            baseTwoTableName,
            baseThreeTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        // do not support pushdown multi exists
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void inPushTest5() throws Exception {
        String sql = MessageFormat.format("select * from (select varchar_test, pk as id from {0} a) a where "
                + "id in (select pk from {1} b where pk = 56 ) "
                + "and id in (select pk from {2} c)",
            baseOneTableName,
            baseTwoTableName,
            baseThreeTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        // do not support pushdown multi exists
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void inPushTest6() throws Exception {
        String sql = MessageFormat.format("select * from {0} a where "
                + "pk in (select pk as id from {1} group by pk having pk = 56)",
            baseOneTableName,
            baseTwoTableName,
            baseThreeTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void inPushTest7() throws Exception {
        String sql = MessageFormat.format("select * "
                + "from (select t1.varchar_test, t2.pk as id from {0} t2 inner join {1} t1 on t2.pk = t1.pk and t1.pk = 56) t3 "
                + "where id in (select pk from {2})",
            baseOneTableName,
            baseTwoTableName,
            baseThreeTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void existsPushTest1() throws Exception {
        String sql = MessageFormat.format(
            "select * from {0} a where exists(select varchar_test from {1} b where a.pk = b.pk) and pk = 56",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void existsPushTest2() throws Exception {
        String sql = MessageFormat.format("select * from {0} a where exists(select varchar_test from "
                + baseTwoTableName + " b where a.pk = b.pk and pk = 56)",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void existsPushTest3() throws Exception {
        String sql = MessageFormat.format(
            "select * from (select varchar_test, pk as id from {0} a) a where exists(select varchar_test from {1} b where a.id = b.pk) and id = 56",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void existsPushTest4() throws Exception {
        String sql = MessageFormat.format("select * from (select varchar_test, pk as id from {0} a) a where "
                + "exists(select varchar_test from {1} b where a.id = b.pk) " + "and id = 56 "
                + "and exists(select * from {2} c where a.id = pk)",
            baseOneTableName,
            baseTwoTableName,
            baseThreeTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void existsPushTest5() throws Exception {
        String sql = MessageFormat.format("select * from (select varchar_test, pk as id from {0} a) a where "
                + "exists(select varchar_test from {1} b where a.id = b.pk and id = 56 ) "
                + "and exists(select * from {2} c where a.id = c.pk)",
            baseOneTableName,
            baseTwoTableName,
            baseThreeTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        // do not support pushdown multi exists
        //if (ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
        //    assertShardCount(tddlConnection, sql, 1);
        //}
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void existsPushTest6() throws Exception {
        String sql = MessageFormat.format("select * from {0} a where "
                + "exists (select count(distinct varchar_test) c from {1} where a.pk = pk group by pk having c = 1 and pk = 56)",
            baseOneTableName,
            baseTwoTableName,
            baseThreeTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void existsPushTest7() throws Exception {
        String sql = MessageFormat.format("select * "
                + "from (select t1.varchar_test, t2.pk as id from {0} t2 inner join {1} t1 on t2.pk = t1.pk and t1.pk = 56) t3 "
                + "where exists (select varchar_test, integer_test, pk from {2} where t3.id = pk)",
            baseOneTableName,
            baseTwoTableName,
            baseThreeTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    @Ignore("correlate subquery with correlate column step over 2 or more tables.")
    public void existsPushTest8() throws Exception {
        String sql = MessageFormat.format("select * "
                + "from (select t1.varchar_test, t2.pk as id from {0} t2 inner join {1} t1 on t2.pk = t1.pk where t1.pk = 56) t3 "
                + "where exists ("
                + "    select varchar_test, integer_test, pk from {2} "
                + "    where t3.id = pk and exists (select * from {3} where t3.varchar_test=varchar_test and t3.id = pk))",
            baseOneTableName,
            baseOneTableName,
            baseTwoTableName,
            baseThreeTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void existsPushTest9() throws Exception {
        String sql = MessageFormat.format(
            "select pk from {0} a where exists(select pk from {1} b where a.pk = b.pk and b.bigint_test > 1) and pk = 56",
            baseOneTableName,
            baseTwoTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        if (!PropertiesUtil.columnarMode() && ConfigUtil.isStrictSameTopology(baseOneTableName, baseTwoTableName)) {
            assertShardCount(tddlConnection, sql, 1);
        }
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void groupByAliasTest0() throws Exception {
        String sql = MessageFormat.format(
            "select * from {0} where pk in (select pk as tinyint_unsigned_test from {1} group by tinyint_unsigned_test) order by pk",
            "all_type_update_delete_base_one_broadcast", // Use different table with different column name.
            baseOneTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void havingAliasTest0() throws Exception {
        String sql = MessageFormat.format(
            "select a.tinyint_test, count(distinct a.pk,a.tinyint_test) from {0} a, {1} b where a.pk=b.integer_test group by a.tinyint_test having a.tinyint_test in "
                +
                "(select distinct xx from (select tinyint_test as xx,count(*) cnt,sum(integer_test) as cnt_2 from {2} group by xx having cnt > 2 and cnt_2 >= 100)tmp) order by 1",
            baseOneTableName,
            baseTwoTableName,
            baseThreeTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.0
     */
    @Test
    public void selectSubQueryUseBkaJoinTest() throws Exception {
        String sql = MessageFormat.format(
            "/*TDDL:cmd_extra(ENABLE_BKA_JOIN=true)*/select (select a.integer_test from {0} a where a.pk = c.integer_test) as pk1,(select b.varchar_test from {1} b where b.pk = c.tinyint_test) as pk2 from {2} c where c.tinyint_1bit_test = 56",
            baseOneTableName,
            baseTwoTableName,
            baseThreeTableName);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * subquery is uncorrelated;
     * outer table is not a split table(by condition).
     * <p>
     * subquery can't be pushed to logicalview, so ignore the case
     */
    @Test
    @FileStoreIgnore
    public void subqueryInProjectTest0() throws Exception {
        if (ConfigUtil.allSingleTable(baseOneTableName, baseTwoTableName)) {
            return;
        }
        String sql = "select pk ,(select varchar_test from " + baseTwoTableName + " where pk = 17 )as name from "
            + baseOneTableName + " where pk=13";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "explain " + sql;
        explainAllResultMatchAssert(sql, null, tddlConnection,
            "[\\s\\S]*" + "individual scalar subquery" + "[\\s\\S]*");
    }

    @Test
    public void testProjectASubquery() {

        String sql = "  select pk,integer_test,(select count(integer_test) from " + baseTwoTableName
            + " where a.varchar_test = varchar_test )  from  " + baseTwoTableName + " a";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, false);

    }

    /**
     * @since 5.1.0
     */
    @Test
    public void applyPushTest() throws Exception {
        if (!ConfigUtil.allSingleTable(baseOneTableName, baseTwoTableName)) {
            return;
        }
        String sql =
            "select * from " + baseOneTableName + " where pk = (select pk from " + baseTwoTableName
                + " where pk < (select pk from " + baseThreeTableName
                + " where pk<100 AND PK >98 limit 1) ORDER BY PK LIMIT 1)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testCorrelateJoin() {
        String sql =
            "select count(b.integer_test), ( select min(integer_test) from "
                + baseOneTableName + " a where a.pk = c.pk group by c.pk) as name from "
                + baseTwoTableName + " b inner join " + baseThreeTableName
                + " c on c.pk = b.pk group by c.integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
