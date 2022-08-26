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
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.dql.sharding.join.JoinUtils.bkaHint;
import static com.alibaba.polardbx.qatest.dql.sharding.join.JoinUtils.bkaWithoutHashJoinHint;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * 三表join测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */

public class ThreeTableJoinTest extends ReadBaseTestCase {

    protected ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4},hint={5}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwoBaseThreeBaseFourWithHint());
    }

    public ThreeTableJoinTest(String baseOneTableName, String baseTwoTableName, String baseThreeTableName,
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
    public void testJoinA() {
        String sql = hint
            + "select a.varchar_test as appname ,a.char_test as char_test, b.varchar_test as hostip,b.char_test as stationid,"
            + "c.varchar_test as hostgroupname from " + baseThreeTableName + " a ," + "" + baseOneTableName + " b ,"
            + baseTwoTableName + " c "
            + "where a.pk = c.integer_test and c.pk = b.integer_test and b.char_test='"
            + columnDataGenerator.char_testValue
            + "'" + " order by appname, char_test, hostip, stationid, hostgroupname";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testJoinA1() {
        String sql = hint + "select a.varchar_test as appname , b.varchar_test as hostip,b.char_test as stationid,"
            + "c.varchar_test as hostgroupname from " + baseOneTableName + " b ," + "" + baseThreeTableName + " a ,"
            + baseTwoTableName + " c "
            + "where c.integer_test = a.pk and b.integer_test = c.pk and a.char_test='"
            + columnDataGenerator.char_testValue + "' and c.integer_test > 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testJoinB() {
        String sql = "select a.varchar_test as appname , b.varchar_test as hostip,b.char_test as stationid,"
            + "c.varchar_test as hostgroupname from " + baseThreeTableName + " a ," + "" + baseOneTableName + " b ,"
            + baseTwoTableName + " c "
            + "where  c.integer_test = a.pk and b.integer_test = c.pk and b.char_test='"
            + columnDataGenerator.char_testValue
            + "'" + " order by appname, hostip, stationid, hostgroupname";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testJoinB1() {
        String sql = "select a.varchar_test as appname , b.varchar_test as hostip,b.char_test as stationid,"
            + "c.varchar_test as hostgroupname from " + baseThreeTableName + " a ," + "" + baseOneTableName + " b ,"
            + baseTwoTableName + " c "
            + "where  c.integer_test = a.pk and b.integer_test = c.pk and b.char_test='"
            + columnDataGenerator.char_testValue
            + "' and a.char_test='" + columnDataGenerator.char_testValue + "' and c.bigint_test >"
            + columnDataGenerator.bigint_testValue;

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testJoinC() {
        String sql = hint + "select a.varchar_test as appname , b.varchar_test as hostip,b.char_test as stationid,"
            + "c.varchar_test as hostgroupname from " + baseThreeTableName + " a ," + "" + baseTwoTableName
            + " c, " + baseOneTableName + " b "
            + "where c.integer_test = a.pk and c.pk = b.integer_test and b.char_test='"
            + columnDataGenerator.char_testValue + "'" + " order by appname, hostip, stationid, hostgroupname";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testJoinC1() {

        String sql = hint + "select a.varchar_test as appname , b.varchar_test as hostip,b.char_test as stationid,"
            + "c.varchar_test as hostgroupname from " + baseThreeTableName + " a ," + "" + baseTwoTableName
            + " c, " + baseOneTableName + " b "
            + "where  c.integer_test = a.pk and b.integer_test = c.pk and b.char_test='"
            + columnDataGenerator.char_testValue + "' and a.char_test='" + columnDataGenerator.char_testValue
            + "' and c.bigint_test >1";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testJoinD() {
        String sql = hint + "select a.varchar_test as appname , b.varchar_test as hostip,b.char_test as stationid,"
            + "c.varchar_test as hostgroupname from " + baseThreeTableName + " a ," + "" + baseTwoTableName
            + " c, " + baseOneTableName + " b "
            + "where  c.integer_test = a.pk and b.integer_test = c.pk and b.char_test='"
            + columnDataGenerator.char_testValue + "'" + " order by appname, hostip, stationid, hostgroupname";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void testJoinD1() {
        String sql = hint + "select a.varchar_test as appname , b.varchar_test as hostip,b.char_test as stationid,"
            + "c.varchar_test as hostgroupname from " + baseThreeTableName + " a ," + "" + baseTwoTableName
            + " c, " + baseOneTableName + " b "
            + "where  c.integer_test = a.pk and b.integer_test = c.pk and b.char_test='"
            + columnDataGenerator.char_testValue + "' and a.char_test='" + columnDataGenerator.char_testValue
            + "' and c.bigint_test> 1";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
    }

    /**
     * @since 5.1.19
     */
    @Test
    public void testJoinD2() {
        if (StringUtils.containsIgnoreCase(hint, "SORT_MERGE_JOIN")) {
            // 不支持sort merge join hint
            return;
        }

        String sql = hint + "select a.varchar_test as appname , b.varchar_test as hostip,b.char_test as stationid,"
            + "c.varchar_test as hostgroupname from " + baseThreeTableName + " a ," + "" + baseTwoTableName
            + " c, " + baseOneTableName + " b "
            + "where c.integer_test > a.pk and b.integer_test = c.pk and b.char_test='"
            + columnDataGenerator.char_testValue + "' and a.char_test='" + columnDataGenerator.char_testValue
            + "' and c.bigint_test>" + columnDataGenerator.bigint_testValue
            + " order by appname, hostip, stationid, hostgroupname";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.24
     */
    @Test
    public void testJoinWithDataTransfer() {
        String[] joinConditions = {
            "a.pk = b.integer_test and b.integer_test = c.pk and c.pk=50",
            "a.pk = b.integer_test and c.pk=50 and b.integer_test = c.pk",
            "c.pk=50 and a.pk = b.integer_test and b.integer_test = c.pk",
            "a.pk = b.integer_test and b.integer_test = c.pk and a.pk=50",
            "a.pk = b.integer_test and a.pk=50 and b.integer_test = c.pk ",
            "a.pk=50 and a.pk = b.integer_test and b.integer_test = c.pk ",
            "a.pk = b.integer_test and b.integer_test = c.pk and b.integer_test=50",
            "a.pk = b.integer_test and b.integer_test=50 and b.integer_test = c.pk ",
            "b.integer_test=50 and a.pk = b.integer_test and b.integer_test = c.pk"};
        for (String joinCondition : joinConditions) {
            String sql = hint + "select * from " + baseThreeTableName + " c ," + "" + baseTwoTableName + " b, "
                + baseOneTableName
                + " a " + "where " + joinCondition;

            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        }
    }

    @Test
    public void testJoinA1WithUnionAndBKA() {
        String sql =
            hint + bkaHint + "select a.varchar_test as appname , b.varchar_test as hostip,b.char_test as stationid,"
                + "c.varchar_test as hostgroupname from " + baseOneTableName + " b ," + "" + baseThreeTableName + " a ,"
                + baseTwoTableName + " c "
                + "where c.integer_test = a.pk and b.integer_test = c.pk and a.char_test='"
                + columnDataGenerator.char_testValue + "' and c.integer_test > 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJoinA1WithUnionAndBKANoHash() {
        String sql = hint + bkaWithoutHashJoinHint
            + "select a.varchar_test as appname , b.varchar_test as hostip,b.char_test as stationid,"
            + "c.varchar_test as hostgroupname from " + baseOneTableName + " b ," + "" + baseThreeTableName + " a ,"
            + baseTwoTableName + " c "
            + "where c.integer_test = a.pk and b.integer_test = c.pk and a.char_test='"
            + columnDataGenerator.char_testValue + "' and c.integer_test > 1";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJoinPushdownWithBroadcastInTheMiddle() {
        String sql = hint + "select * from " + baseOneTableName + " t2 ," + "" + baseThreeTableName + " t1 ,"
            + baseTwoTableName + " t3 "
            + "where t1.integer_test = t2.integer_test and t2.pk = t3.pk and t2.pk between 100 and 130;";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJoinPushdownWithoutShardingKeyOutput1() {
        String sql = hint + "SELECT *\n"
            + "  FROM (\n"
            + "        SELECT a.varchar_test,\n"
            + "               b.pk AS id,\n"
            + "               a.integer_test\n"
            + "          FROM " + baseOneTableName + " a join " + baseThreeTableName + " b on a.pk = b.pk\n"
            + "       ) a\n"
            + " WHERE exists(\n"
            + "        SELECT varchar_test\n"
            + "          FROM " + baseTwoTableName + " b\n"
            + "         WHERE a.id = b.pk\n"
            + "       )\n"
            + "   AND id between 100 and 130\n";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testJoinPushdownWithoutShardingKeyOutput2() {
        String sql = hint + "SELECT *\n"
            + "  FROM " + baseTwoTableName + " b\n"
            + "  JOIN (\n"
            + "        SELECT a.varchar_test,\n"
            + "               b.pk AS id,\n"
            + "               a.integer_test\n"
            + "          FROM " + baseOneTableName + " a\n"
            + "          JOIN " + baseThreeTableName + " b\n"
            + "            ON a.pk = b.pk\n"
            + "       ) a\n"
            + "    ON a.id = b.pk\n"
            + " WHERE b.pk BETWEEN 100 AND 130\n";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
