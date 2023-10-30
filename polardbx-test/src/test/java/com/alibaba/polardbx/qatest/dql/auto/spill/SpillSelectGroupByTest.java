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
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectErrorAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;


public class SpillSelectGroupByTest extends AutoReadBaseTestCase {

    String hint = " /*+TDDL:cmd_extra(ENABLE_SPILL=true)*/";

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SpillSelectGroupByTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    /**
     * @since 5.1.17
     */
    @Test
    public void greaterTest() throws Exception {
        String sql = hint + "select pk,sum(pk) from " + baseOneTableName + " group by pk order by sum(pk) desc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.17
     */
    @Test
    @Ignore
    public void groupbyNumberTest() throws Exception {
        String sql = hint + "select pk,sum(pk) from " + baseOneTableName + " group by 1 order by sum(pk) desc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-1
     */
    @Test
    public void groupbyTwoNumberTest() throws Exception {
        String sql = hint + "select pk, integer_test, sum(pk) from " + baseOneTableName
            + " group by pk , integer_test order by sum(pk) desc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-1
     */
    @Test
    public void groupbyTwoNumberOrderbyNumTest() throws Exception {
        String sql = hint + "select pk, integer_test, sum(pk) from " + baseOneTableName
            + " group by pk , integer_test order by 3 desc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-1
     */
    @Test
    public void groupbyTwoNumberOrderbyTwoNumTest() throws Exception {
        String sql = hint + "select pk, integer_test, sum(pk) from " + baseOneTableName
            + " group by pk, integer_test order by 2 desc, 3 asc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * @since 5.1.25-1
     */
    @Test
    @Ignore("错误不兼容")
    public void groupbyNumberWithNumberNotExistTest() throws Exception {
        String sql = hint + "select pk, integer_test, sum(pk) from " + baseOneTableName
            + " group by pk, integer_test order by 4 asc";
        selectErrorAssert(sql, null, tddlConnection, "Unknown column");
    }

    /**
     * @since 5.1.25-1
     */
    @Test
    public void groupbyNumberWithSubQueryTest() throws Exception {
        String sql = hint + "select pk, sum(integer_test) from (select pk, integer_test from " + baseOneTableName
            + "  where integer_test > 10 order by 1)a group by pk";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Order by Expressions are not in Group by list or Select list.
     */
    @Test
    public void groupByWithDifferentOrderBy() throws Exception {
        String sql = hint + "select count(pk), varchar_test from " + baseOneTableName
            + " group by varchar_test order by integer_test";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * Order by agg Expressions that in Group by list .
     */
    @Test
    public void groupByWithOrderByAgg() throws Exception {
        String sql = hint + "select count(pk), floor(pk / 10) from " + baseOneTableName
            + " where pk>1 group by floor(pk/10) order by floor(pk/10) desc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void groupByWithOrderByAgg2() throws Exception {
        String sql = hint + "select count(pk), abs(pk - 10) from " + baseOneTableName
            + " group by abs(pk-10) order by abs(pk-10) desc";
        selectOrderAssert(sql, null, mysqlConnection, tddlConnection);
    }

    /**
     * bug fixed cast error,like
     * agg
     * agg
     * LogicalView
     */
    @Test
    public void groupByWithGroupBy() throws Exception {
        String sql = hint
            + "/*+TDDL:cmd_extra(MERGE_CONCURRENT=true,PARALLELISM=2)*/ SELECT   c_count,   count(*) AS custdist FROM (SELECT char_test,count(time_test) AS c_count   from "
            +
            baseOneTableName
            + "  where varchar_test NOT LIKE '%special%requests%' GROUP BY char_test)  c_orders   GROUP BY c_count   ORDER BY custdist DESC, c_count desc";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}

