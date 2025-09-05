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

package com.alibaba.polardbx.qatest.dql.sharding.join;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.polardbx.qatest.dql.sharding.join.JoinUtils.bkaHint;
import static com.alibaba.polardbx.qatest.dql.sharding.join.JoinUtils.bkaWithoutHashJoinHint;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * 内连接测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */


public class InnerJoinTest extends ReadBaseTestCase {

    private static AtomicBoolean shardingAdvise = new AtomicBoolean(false);

    protected ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4},hint={5}")
    public static List<String[]> prepareDate() {
        List<String[]> ret = new ArrayList<>();
        ret.addAll(Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwoBaseThreeBaseFourWithHint()));
        ret.addAll(Arrays.asList(ExecuteTableSelect.selectFourTableWithRuntimeFilter()));
        return ret;
    }

    public InnerJoinTest(String baseOneTableName, String baseTwoTableName, String baseThreeTableName,
                         String baseFourTableName, String hint) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
        this.baseThreeTableName = baseThreeTableName;
        this.baseFourTableName = baseFourTableName;
        this.hint = hint;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void innerJoinTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test from " + baseOneTableName + " inner join "
                + baseTwoTableName
                + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void innerJoinMultiUsingTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test from " + baseOneTableName + " inner join "
                + baseTwoTableName
                + "  " + "using(pk, integer_test, varchar_test)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        if (shardingAdvise.compareAndSet(false, true)) {
            sql = "/*+TDDL:cmd_extra(SHARDING_ADVISOR_BROADCAST_THRESHOLD=100)*/shardingadvise";
            DataValidator.sqlMayErrorAssert(sql, tddlConnection, "ERR_TABLE_NOT_EXIST");
        }
    }

    @Test
    public void innerJoinMultiUsingLimitTest() {
        String sql = hint + "select " + "a" + ".pk," + "a" + ".varchar_test," + "a"
            + ".integer_test," + "b" + ".varchar_test from " + "((select * from " + baseOneTableName
            + " order by pk limit 10) a" + " inner join " + "(select * from " + baseTwoTableName + ") b"
            + "  " + "using(pk, integer_test, varchar_test))";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void innerJoinUsingTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test from " + baseOneTableName + " inner join "
                + baseTwoTableName
                + "  " + "using(pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void innerJoinWithMultiOnTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test from " + baseOneTableName + " inner join "
                + baseTwoTableName
                + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk and " + baseOneTableName
                + ".varchar_test=" + baseTwoTableName + ".varchar_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithMutilValueTest() {
        String sql = hint + "select * from " + baseTwoTableName + " inner join " + baseOneTableName + "  " + "on "
            + baseOneTableName
            + ".integer_test=" + baseTwoTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithAndTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".integer_test," + baseOneTableName
                + ".varchar_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " inner join "
                + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName
                + ".pk where "
                + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereNumFieldTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " inner join "
                + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName
                + ".pk where "
                + baseOneTableName + ".pk=0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
            + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " inner join "
            + baseOneTableName
            + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName + ".pk where " + baseOneTableName
            + ".pk>0";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereStringFieldTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " inner join "
                + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName
                + ".pk where "
                + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereAndTest() {
        String sql =
            hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " inner join "
                + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName
                + ".pk where "
                + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "' and "
                + baseTwoTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereOrTest() {
        String sql = hint + "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test,"
            + baseOneTableName
            + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName + " inner join "
            + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName
            + ".pk where "
            + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "' or "
            + baseTwoTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithWhereLimitTest() {
        // 暂时先去掉对baseOneTableName_oneGroup_oneAtom_threeIndex这个表类型的验证，应该是因为没有异步复制导致的
        if (baseOneTableName != hint + "baseOneTableName_oneGroup_oneAtom_threeIndex") {
            String sql =
                "select " + baseOneTableName + ".pk," + "" + baseOneTableName + ".varchar_test," + baseOneTableName
                    + ".integer_test," + baseTwoTableName + ".varchar_test " + "from " + baseTwoTableName
                    + " inner join "
                    + baseOneTableName + "  " + "on " + baseOneTableName + ".integer_test=" + baseTwoTableName
                    + ".pk where "
                    + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValue + "'";
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableTest() {
        String sql =
            hint + "SELECT * FROM " + baseOneTableName + " inner JOIN " + baseTwoTableName + " ON " + baseOneTableName
                + ".integer_test="
                + baseTwoTableName + ".pk " + "inner JOIN " + baseThreeTableName + " ON " + baseTwoTableName + ".pk="
                + baseThreeTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableWithWhere() {
        String sql =
            hint + "SELECT * from " + baseOneTableName + " INNER JOIN " + baseTwoTableName + " ON " + baseOneTableName
                + ".pk=" + baseTwoTableName + ".integer_test INNER JOIN " + baseThreeTableName + "  ON "
                + baseThreeTableName
                + ".pk=" + baseTwoTableName + ".integer_test where " + baseThreeTableName + ".varchar_test='"
                + columnDataGenerator.varchar_testValue + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableWithWhereWithOr() {
        String sql =
            hint + "SELECT * from " + baseOneTableName + " INNER JOIN " + baseTwoTableName + " ON " + baseOneTableName
                + ".pk=" + baseTwoTableName + ".integer_test INNER JOIN " + baseThreeTableName + "  ON "
                + baseThreeTableName
                + ".PK=" + baseTwoTableName + ".integer_test where " + baseThreeTableName + ".varchar_test='"
                + columnDataGenerator.varchar_testValue + "' or "
                + baseThreeTableName + ".varchar_test='" + columnDataGenerator.varchar_testValueTwo + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableWithWhereWithAnd() {
        String sql =
            hint + "SELECT * from " + baseOneTableName + " INNER JOIN " + baseTwoTableName + " ON " + baseOneTableName
                + ".pk=" + baseTwoTableName + ".integer_test INNER JOIN " + baseThreeTableName + "  ON "
                + baseThreeTableName
                + ".pk=" + baseTwoTableName + ".integer_test where " + baseThreeTableName + ".varchar_test='"
                + columnDataGenerator.varchar_testValue + "' or "
                + baseOneTableName + ".varchar_test='" + columnDataGenerator.varchar_testValueTwo + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableMutilDateTest() {
//        String sql = "replace into " + baseOneTableName + "(host_id,hostgroup_id) values(?,?)";
//        List<Object> param = new ArrayList<Object>();
//        for (long i = 3; i < 8; i++) {
//            param.clear();
//            param.add(i);
//            param.add(0l);
//            tddlUpdateData(sql, param);
//            mysqlUpdateData(sql, param);
//        }
        String sql =
            hint + "SELECT * FROM " + baseOneTableName + " inner JOIN " + baseTwoTableName + " ON " + baseOneTableName
                + ".integer_test="
                + baseTwoTableName + ".pk " + "inner JOIN " + baseThreeTableName + " ON " + baseTwoTableName + ".pk="
                + baseThreeTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithAliasTest() {
        String sql = hint + "select a.pk,a.varchar_test,a.integer_test,b.varchar_test from " + baseOneTableName
            + " a inner join " + baseTwoTableName + " b " + "on a.integer_test=b.pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithAliasAsTest() {
        String sql = hint + "select a.pk,a.varchar_test,a.integer_test,b.varchar_test from " + baseOneTableName
            + " as a inner join " + baseTwoTableName + " as b " + "on a.integer_test=b.pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithOrderByTest() {
        String sql = hint + "select a.pk,a.varchar_test,a.integer_test,b.varchar_test from " + baseOneTableName
            + " as a inner join " + baseTwoTableName + " as b "
            + "on a.integer_test=b.pk order by a.pk";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinWithOrderByascTest() {
        String sql = hint + "select a.pk,a.varchar_test,a.integer_test,b.varchar_test from " + baseOneTableName
            + " as a inner join " + baseTwoTableName + " as b "
            + "on a.integer_test=b.pk order by a.pk asc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test()
    public void InnerJoinWithOrderBydescTest() {
        String sql = hint + "select a.pk,a.varchar_test,a.integer_test,b.varchar_test from " + baseOneTableName
            + " as a inner join " + baseTwoTableName + " as b "
            + "on a.integer_test=b.pk order by a.pk desc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test()
    public void InnerJoinWithSubQueryTest() {
        String sql =
            hint + "select t1.sum1,t2.count2 from (select sum(pk) as sum1,integer_test from " + baseOneTableName
                + " group by integer_test) t1 " + "join (select count(pk) as count2,integer_test from "
                + baseTwoTableName + " group by integer_test) t2 on t1.integer_test=t2.integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test()
    public void InnerJoinWithSubQueryMultiJoinTest() {
        String sql =
            hint + "select t1.sum1,t2.count2 from (select sum(pk) as sum1,integer_test from " + baseOneTableName
                + " group by integer_test) t1 " + "join (select count(pk) as count2,integer_test from "
                + baseTwoTableName
                + " group by integer_test) t2 on t1.integer_test=t2.integer_test and t1.sum1 >t2.count2";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test()
    public void InnerJoinWithUsingSubQueryMultiJoinTest() {
        String sql =
            hint + "select t1.sum1,t2.count2 from (select sum(pk) as sum1,integer_test from " + baseOneTableName
                + " group by integer_test) t1 " + "join (select count(pk) as count2,integer_test from "
                + baseTwoTableName + " group by integer_test) t2 using(integer_test) where  t1.sum1 >t2.count2";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Ignore("ungrouped")
    public void InnerJoinWithAvgTest() {
        String sql = hint + "select t1.avg,t2.avg,avg(t1.`integer_test`) from (select avg(pk) as avg,integer_test from "
            + baseOneTableName + " group by integer_test) t1 "
            + "join (select avg(pk) as avg,integer_test from " + baseTwoTableName
            + " group by integer_test) t2 on t1.integer_test=t2.integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Ignore("ungrouped")
    public void InnerJoinWithAvgAndGroupByNumberTest() {
        String sql = hint + "select t1.avg,t2.avg,avg(t1.`integer_test`) from (select avg(pk) as avg,integer_test from "
            + baseOneTableName + " group by 2) t1 " + "join (select avg(pk) as avg,integer_test from "
            + baseTwoTableName + " group by 2) t2 on t1.integer_test=t2.integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     *
     */
    @Test
    public void InnerJoinWithRePushWhereFilters() {
        String sql = "";

        // 三表join
        sql = "SELECT so.pk FROM " + baseOneTableName + " cs,  " + baseTwoTableName + " so,  " + baseThreeTableName
            + " ss WHERE ss.`pk` = so.`pk` AND cs.`pk` = so.`pk` AND so.`pk` = '100';";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        // 四表join
        sql = "SELECT d.pk FROM  " + baseOneTableName + " a,  " + baseTwoTableName + " b,  " + baseThreeTableName
            + " c,  " + baseFourTableName + " d "
            + "WHERE b.`pk` = c.`pk` AND c.`pk` = a.`pk` AND c.`pk` = '100' AND a.`pk` = d.`pk`;";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        // 三表join，第三个表含有union
        sql = "SELECT a.pk " + "FROM  " + baseOneTableName + " a,  " + baseTwoTableName + " b, "
            + " ( (select pk from  " + baseThreeTableName + " c where c.pk=100) " + "  union distinct "
            + " (select pk from   " + baseFourTableName + " d where d.pk=100)) e " + " WHERE b.`pk` = e.`pk` "
            + "  AND e.`pk` = a.`pk` " + "  AND e.`pk` = '100'; ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     *
     */
    @Test
    public void InnerJoinWithDifferentTopologies() {
        String sql = "";

        sql = "select e.pk as pk, f.pk  from  select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX
            + " e join   select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX + "  f where e.pk=f.pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select e.pk as pk, f.pk  from  select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX
            + "  e join   select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX
            + "  f where e.pk=f.pk and f.pk=5";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     *
     */
    @Test
    public void InnerJoinWithUsingDifferentTopologies() {
        String sql = "";

        sql = "select e.pk as pk, f.pk  from  select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX
            + " e join   select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX + "  f using(pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select e.pk as pk, f.pk  from  select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX
            + "  e join   select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX + "  f using(pk) where f.pk=5";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     *
     */
    @Test
    public void InnerJoinWithDifferentTopologies2() {
        String sql = "";

        sql = "select e.pk as pk, f.pk  from  " + baseOneTableName + " e join  " + baseTwoTableName
            + "  f where e.pk=f.pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select e.pk as pk, f.pk  from " + baseOneTableName + "  e join   " + baseTwoTableName
            + "  f where e.pk=f.pk and f.pk=5";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     *
     */
    @Test
    public void InnerJoinWithUsingDifferentTopologies2() {
        String sql = "";

        sql = "select e.pk as pk, f.pk  from  " + baseOneTableName + " e join  " + baseTwoTableName + "  f using(pk)";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = "select e.pk as pk, f.pk  from " + baseOneTableName + "  e join   " + baseTwoTableName
            + "  f using(pk) where f.pk=5";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void InnerJoinWithBKAAndMergeUnionOptimization() {
        String sql = "";

        sql = bkaHint + "select e.pk as pk, f.pk  from  select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX
            + " e join   select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX + "  f where e.pk=f.pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = bkaHint + "select e.pk as pk, f.pk  from  select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX
            + "  e join   select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX
            + "  f where e.pk=f.pk and f.pk=5";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void InnerJoinWithBKAAndMergeUnionOptimizationNoHash() {
        String sql = "";
        sql = bkaWithoutHashJoinHint + "select e.pk as pk, f.pk  from  select_base_two_"
            + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX + " e join   select_base_one_"
            + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX + "  f where e.pk=f.pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = bkaWithoutHashJoinHint + "select e.pk as pk, f.pk  from  select_base_two_"
            + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX + "  e join   select_base_one_"
            + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX + "  f where e.pk=f.pk and f.pk=5";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void InnerJoinWithBKAAndMergeSort() {
        String sql = "";

        sql = bkaHint + "select e.pk as pk, f.pk  from  (select * from select_base_two_"
            + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX + " order by pk limit 0,1) e join   select_base_one_"
            + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX + "  f where e.pk=f.pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableWithUnionAndBKATest() {
        String sql = hint + bkaHint + "SELECT * FROM " + baseOneTableName + " inner JOIN " + baseTwoTableName + " ON "
            + baseOneTableName + ".integer_test="
            + baseTwoTableName + ".pk " + "inner JOIN " + baseThreeTableName + " ON " + baseTwoTableName + ".pk="
            + baseThreeTableName + ".pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void InnerJoinThreeTableWithWhereAndUnionAndBKA() {
        String sql = hint + bkaHint + "SELECT * from " + baseOneTableName + " INNER JOIN " + baseTwoTableName + " ON "
            + baseOneTableName
            + ".pk=" + baseTwoTableName + ".integer_test INNER JOIN " + baseThreeTableName + "  ON "
            + baseThreeTableName
            + ".pk=" + baseTwoTableName + ".integer_test where " + baseThreeTableName + ".varchar_test='"
            + columnDataGenerator.varchar_testValue + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void InnerJoinThreeTableWithWhereAndUnionAndBKANoHash() {
        String sql =
            hint + bkaWithoutHashJoinHint + "SELECT * from " + baseOneTableName + " INNER JOIN " + baseTwoTableName
                + " ON " + baseOneTableName
                + ".pk=" + baseTwoTableName + ".integer_test INNER JOIN " + baseThreeTableName + "  ON "
                + baseThreeTableName
                + ".pk=" + baseTwoTableName + ".integer_test where " + baseThreeTableName + ".varchar_test='"
                + columnDataGenerator.varchar_testValue + "'";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void innerJoinWithSubqueryTest() {
        String sql =
            "select * from (select c.integer_test as pk from " + baseOneTableName + " c join " + baseTwoTableName
                + " b on c.pk = b.pk) a where a.pk = 3;";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void CBOBKAJoinNotSimpleConditionTest() {

        String tableOne = "select_base_one_multi_db_multi_tb";
        String tableTwo = "select_base_one_multi_db_one_tb";

        String sql = "/*+TDDL:cmd_extra(enable_cbo=true) BKA_JOIN(" + tableOne + "," + tableTwo + ")*/ "
            + "select " + tableOne + ".pk," + tableOne + ".varchar_test," + tableOne
            + ".integer_test," + tableTwo + ".varchar_test from " + tableOne + " inner join "
            + " ( select pk + 1 as pk, varchar_test from  " + tableTwo
            + ")  " + tableTwo + " on " + tableOne + ".integer_test=" + tableTwo + ".pk";

        selectErrorAssert(sql, null, tddlConnection, "Sql could not be implemented");
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void BKAJoinSimpleConditionTest() {
        String tableOne = "select_base_one_multi_db_multi_tb";
        String tableTwo = "select_base_one_multi_db_one_tb";

        String sql = "/*+TDDL: BKA_JOIN(" + tableOne + "," + tableTwo + ")*/ "
            + "select " + tableOne + ".pk," + tableOne + ".varchar_test," + tableOne
            + ".integer_test," + tableTwo + ".varchar_test from " + tableOne + " inner join "
            + " ( select pk as pk, varchar_test from  " + tableTwo
            + ")  " + tableTwo + " on " + tableOne + ".integer_test=" + tableTwo + ".pk";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        String explainSql = "explain " + sql;
        String explainResult = explainResultString(explainSql, null, tddlConnection);
        Assert.assertTrue(explainResult.indexOf("NLJoin") == -1);
    }

    @Test
    public void bkaJoinLookupCondTest() {
        String tableOne = "select_base_one_multi_db_multi_tb";
        String tableTwo = "select_base_two_multi_db_multi_tb";
        String tableThree = "select_base_three_multi_db_multi_tb";

        String sql = String.format(" /*+ TDDL: bka_join(%s, %s)*/ "
                + "select * from %s a, %s b, %s c where"
                + " c.pk = b.pk and c.mediumint_test = b.mediumint_test and b.integer_test = a.mediumint_test and a.mediumint_test = 10;",
            tableOne, tableTwo, tableOne, tableTwo, tableThree);

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        String explainResult = explainResultString("explain " + sql, null, tddlConnection);
        Assert.assertTrue(explainResult.contains("BKAJoin"));
    }

    private String explainResultString(String sql, List<Object> param, Connection tddlConnection) {
        PreparedStatement tddlPs = JdbcUtil.preparedStatementSet(sql, param, tddlConnection);
        StringBuilder actualExplainResult = new StringBuilder();
        ResultSet rs = null;
        try {
            rs = tddlPs.executeQuery();
            while (rs.next()) {
                actualExplainResult.append(rs.getString(1));
            }
            return actualExplainResult.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(tddlPs);
        }
    }
}
