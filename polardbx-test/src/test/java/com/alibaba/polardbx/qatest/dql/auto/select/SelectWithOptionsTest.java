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
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectErrorAssert;

/**
 * @author chenghui.lch 2016年2月22日 下午3:25:05
 * @since 5.0.0
 */

public class SelectWithOptionsTest extends AutoReadBaseTestCase {

    @Parameters(name = "{index}:table1={0}")
    public static List<String[]> prepareData() {
        // 因为SqlCalcFoundRows 目前只支持 单库单表 的下推
        // , 所以这里只先择目前支持的表进行查询测试
        // 单库单表无规则、单库单表有规则二类型的表进行测试
        return Arrays.asList(ExecuteTableSelect.selectBaseOneOneDbOneTb());
    }

    public SelectWithOptionsTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Test
    @Ignore
    public void testSelectWithSqlCalcFoundRows() throws Exception {

        String sql = "select sql_calc_found_rows pk, varchar_test from " + baseOneTableName
            + " where pk in (1,2,3) limit 1;";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "select found_rows() as ALL_ROWS;";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        sql = "select found_rows() as ALL_ROWS";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    @Ignore
    public void testSelectWithSqlCalcFoundRowsOnNoRuleSingleTable() throws Exception {

        String sql1 =
            "select sql_calc_found_rows pk, varchar_test from select_with_no_rule where pk in (1,2,3) limit 1;";
        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection, true);

        String sql2 = "select found_rows() as ALL_ROWS;";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);

        String sql3 = "select found_rows() as ALL_ROWS";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);

    }

    @Test
    @Ignore
    public void testMultiSqlSelectWithSqlCalcFoundRows() throws Exception {

        String sql = "select sql_calc_found_rows pk, varchar_test from " + baseOneTableName
            + " where pk in (1,2,3) limit 1; select pk, varchar_test from " + baseOneTableName + " limit 10;";
        selectErrorAssert(sql, null, tddlConnection, "NOT support");
    }

    @Test
    @Ignore
    public void testSelectUnionWithSqlCalcFoundRows() throws Exception {

        // ============union=============
        String sql1 = " ( select sql_calc_found_rows pk, varchar_test from " + baseOneTableName
            + " where pk in (1,2) ) UNION ( select pk, varchar_test from " + baseOneTableName
            + " where pk in (3,6,7) ) order by pk limit 2;";

        selectContentSameAssert(sql1, null, mysqlConnection, tddlConnection);

        String sql2 = "select found_rows() as ALL_ROWS;";
        selectContentSameAssert(sql2, null, mysqlConnection, tddlConnection);

        String sql3 = "select found_rows() as ALL_ROWS";
        selectContentSameAssert(sql3, null, mysqlConnection, tddlConnection);

        // ============union all=============
        String sql4 = " ( select sql_calc_found_rows pk, varchar_test from " + baseOneTableName
            + " where pk in (1,2) ) UNION ALL ( select pk, varchar_test from " + baseOneTableName
            + " where pk in (3,6,7) ) order by pk limit 2;";
        selectContentSameAssert(sql4, null, mysqlConnection, tddlConnection);

        String sql5 = "select found_rows() as ALL_ROWS;";
        selectContentSameAssert(sql5, null, mysqlConnection, tddlConnection);

        String sql6 = "select found_rows() as ALL_ROWS";
        selectContentSameAssert(sql6, null, mysqlConnection, tddlConnection);

    }

}
