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

package com.alibaba.polardbx.qatest.dql.auto.spill;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

public class HybridInnerJoinTest extends AutoReadBaseTestCase {

    protected ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4},hint={5}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(selectBaseOneBaseTwoBaseThreeBaseFourWithHint());
    }

    public static String[][] selectBaseOneBaseTwoBaseThreeBaseFourWithHint() {
        String mpp_hint = " /*+TDDL:cmd_extra(ENABLE_SPILL=true)*/";

        String[][] object = {
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, mpp_hint},
            {
                "select_base_one_" + ExecuteTableName.BROADCAST_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.ONE_DB_ONE_TB_SUFFIX, mpp_hint},

            // case[4]
            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MULTI_DB_ONE_TB_SUFFIX, mpp_hint},

            // case[9]
            {
                "select_base_one_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, mpp_hint},

            {
                "select_base_one_" + ExecuteTableName.ONE_DB_MUTIL_TB_SUFFIX,
                "select_base_two_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_three_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX,
                "select_base_four_" + ExecuteTableName.MUlTI_DB_MUTIL_TB_SUFFIX, mpp_hint},};
        return object;
    }

    public HybridInnerJoinTest(String baseOneTableName, String baseTwoTableName, String baseThreeTableName,
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
}
