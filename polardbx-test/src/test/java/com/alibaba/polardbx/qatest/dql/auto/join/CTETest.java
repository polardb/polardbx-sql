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

package com.alibaba.polardbx.qatest.dql.auto.join;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssertWithDiffSql;

/**
 * @author dylan
 */

public class CTETest extends AutoReadBaseTestCase {

    @Parameterized.Parameters(
        name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4},table5={5},table6={6},table7={7}," +
            "table8={8},table9={9},table10={10},table11={11},table12={12},table13={13},table14={14},table15={15}," +
            "table16={16},table17={17},table18={18},table19={19}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBushyJoinTestTable());
    }

    String[] group1;
    String[] group2;
    String[] group3;
    String[] group4;
    String[] group5;
    String[][] groupList;

    public CTETest(String g1tb1, String g1tb2, String g1tb3, String g1tb4,
                   String g2tb1, String g2tb2, String g2tb3, String g2tb4,
                   String g3tb1, String g3tb2, String g3tb3, String g3tb4,
                   String g4tb1, String g4tb2, String g4tb3, String g4tb4,
                   String g5tb1, String g5tb2, String g5tb3, String g5tb4) {
        /**
         *  tables from same group have same rule
         *  tables from different group have different rule
         */
        group1 = new String[] {g1tb1, g1tb2, g1tb3, g1tb4};
        group2 = new String[] {g2tb1, g2tb2, g2tb3, g2tb4};
        group3 = new String[] {g3tb1, g3tb2, g3tb3, g3tb4};
        group4 = new String[] {g4tb1, g4tb2, g4tb3, g4tb4};
        group5 = new String[] {g5tb1, g5tb2, g5tb3, g5tb4};
        groupList = new String[][] {group1, group2, group3, group4, group5};
    }

    @Test
    public void singleTableTest() {
        String tddlSql = "with cte as (select pk from " + group1[1] + " ) select pk from cte";
        String mysqlSql = "select pk from " + group1[1];
        selectContentSameAssertWithDiffSql(tddlSql, mysqlSql, null, mysqlConnection, tddlConnection, false, false,
            false);
    }

    @Test
    public void twoTableTest() {
        String tddlSql = "with cte as (select pk from " + group1[1] + " ), cte2 as (select pk from " + group2[1]
            + ") select cte.pk from cte join cte2 on cte.pk = cte2.pk";
        String mysqlSql =
            "select cte.pk from " + group1[1] + " as cte join " + group2[1] + " as cte2 on cte.pk = cte2.pk";
        selectContentSameAssertWithDiffSql(tddlSql, mysqlSql, null, mysqlConnection, tddlConnection, false, false,
            false);
    }

    @Test
    public void aggTest() {
        String tddlSql =
            "with cte as (select pk from " + group1[1] + " ), cte2 as (select count(pk) from cte) select * from cte2";
        String mysqlSql = "select count(pk) from " + group1[1];
        selectContentSameAssertWithDiffSql(tddlSql, mysqlSql, null, mysqlConnection, tddlConnection, false, false,
            false);
    }
}

