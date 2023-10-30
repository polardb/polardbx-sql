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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectConutAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

/**
 * Right Join测试
 *
 * @author zhuoxue
 * @since 5.0.1
 */


public class RightJoinAliasTest extends ReadBaseTestCase {

    protected ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table0={0},table1={1},hint={2}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect
            .selectBaseOneBaseOneWithHint());
    }

    public RightJoinAliasTest(String baseOneTableName, String baseTwoTableName,
                              String hint) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
        this.hint = hint;
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void rightJoinTest() {
        String sql =
            hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " + baseTwoTableName + " a "
                + "right join " + baseOneTableName + " b " + "on a.integer_test=b.integer_test  where b.pk < 100";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void rightJoinWithWhereTest() {
        String sql =
            hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " + baseTwoTableName + " a "
                + "right join " + baseOneTableName + " b " + "on a.integer_test=b.integer_test  where a.varchar_test='"
                + columnDataGenerator.varchar_testValue + "'";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void rightJoinWithAndTest() {
        String sql =
            hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " + baseTwoTableName + " a "
                + "right join " + baseOneTableName + " b " + "on a.integer_test=b.integer_test  where a.varchar_test='"
                + columnDataGenerator.varchar_testValue + "' and b.varchar_test='"
                + columnDataGenerator.varchar_testValue + "'";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void rightJoinWithOrTest() {
        String sql =
            hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " + baseTwoTableName + " a "
                + "right join " + baseOneTableName + " b " + "on a.integer_test=b.integer_test  where a.varchar_test='"
                + columnDataGenerator.varchar_testValue + "' or b.varchar_test='"
                + columnDataGenerator.varchar_testValueTwo + "'";

        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void rightJoinWithLimitTest() {
        String sql =
            hint + "select a.pk, a.varchar_test, a.integer_test,b.varchar_test from " + baseTwoTableName + " a "
                + "right join " + baseOneTableName + " b " + "on a.integer_test=b.integer_test  where a.varchar_test='"
                + columnDataGenerator.varchar_testValue + "' limit 1";

        selectConutAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void rightJoinWithAliasTest() {
        String sql = hint + "select a.pk,a.varchar_test,a.integer_test,b.varchar_test from  " + baseOneTableName
            + " a right join  " + baseTwoTableName + " b " + "on b.integer_test=a.integer_test  where  b.pk<100 ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void rightJoinWithAliasAsTest() {
        String sql = hint + "select a.pk,a.varchar_test,a.integer_test,b.varchar_test from " + baseOneTableName
            + " as a right join  " + baseTwoTableName + " as b " + "on b.integer_test=a.integer_test  where a.pk<100 ";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    public void rightJoinWithOrderByTest() {
        String sql = hint
            + "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select a.pk,a.varchar_test,a.integer_test,b.varchar_test from "
            + baseOneTableName + " as a right join " + baseTwoTableName + " as b "
            + "on b.pk=a.integer_test where b.varchar_test='" + columnDataGenerator.varchar_testValue
            + "' order by a.pk";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
        sql = hint
            + "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select a.pk,a.varchar_test,a.integer_test,b.varchar_test from "
            + baseOneTableName + " as a right join " + baseTwoTableName + " as b "
            + "on b.pk=a.integer_test where b.varchar_test='" + columnDataGenerator.varchar_testValue
            + "' order by a.pk asc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
        sql = hint
            + "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select a.pk,a.varchar_test,a.integer_test,b.varchar_test from "
            + baseOneTableName + " as a right join " + baseTwoTableName + " as b "
            + "on b.pk=a.integer_test where b.varchar_test='" + columnDataGenerator.varchar_testValue
            + "' order by a.pk desc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.0.1
     */
    @Test
    @Ignore("bug")
    public void rightJoinWithSubQueryTest() {
        // {
        // String sql =
        // "/* ANDOR ALLOW_TEMPORARY_TABLE=True */select  sum(pk) as sumId,varchar_test,integer_test from "
        // + baseOneTableName
        // +
        // " where pk BETWEEN ? and ? GROUP BY varchar_test ORDER BY integer_test LIMIT ?";
        // List<Object> param = new ArrayList<Object>();
        // param.add(0);
        // param.add(100);
        // param.add(10);
        // // param.add(1);
        // // param.add(20);
        // String[] columnParam = { "sumId", "varchar_test", "integer_test" };
        // selectContentSameAssert(sql, columnParam, param);
        // }

        {
            String sql = hint
                + "/* ANDOR ALLOW_TEMPORARY_TABLE=True */SELECT sumId , name, a.integer_test as aid ,b.pk  as bid from "
                + "( select  sum(pk) as sumId,varchar_test   as name,integer_test from "
                + baseOneTableName
                + " where pk BETWEEN ? and ? GROUP BY varchar_test ORDER BY integer_test LIMIT ?) as a "
                + "RIGHT JOIN (SELECT SUM(pk) , varchar_test,pk  from "
                + baseTwoTableName
                + " where pk "
                + "BETWEEN ? and ? GROUP BY varchar_test ) as b ON a.integer_test=b.pk ORDER BY sumId DESC";
            List<Object> param = new ArrayList<Object>();
            param.add(0);
            param.add(100);
            param.add(10);
            param.add(1);
            param.add(20);
            selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
        }
    }
}
