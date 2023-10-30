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

package com.alibaba.polardbx.qatest.dql.sharding.hint;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

public class NodeHintTest extends ReadBaseTestCase {

    @Parameterized.Parameters(
        name = "{index}:table0={0},table1={1},table2={2},table3={3},table4={4},table5={5},table6={6},table7={7}," +
            "table8={8},table9={9},table10={10},table11={11},table12={12},table13={13},table14={14},table15={15}," +
            "table16={16},table17={17},table18={18},table19={19}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectBushyJoinTestTable());
    }

    // SINGLE_TB
    String[] group1;
    // MULTI_DB_ONE_TB
    String[] group2;

    // ONE_DB_MUTIL_TB
    String[] group3;
    // MUlTI_DB_MUTIL_TB
    String[] group4;

    // BROADCAST_TB
    String[] group5;

    public NodeHintTest(String g1tb1, String g1tb2, String g1tb3, String g1tb4,
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
    }

    @Test
    public void testNodeHint() {
        String joinSql = "select * from %s a join %s b on a.pk = b.pk join %s c on a.integer_test = c.integer_test";
        String subQuerySql = "select * from %s a where a.integer_test not in "
            + "(select c.pk from %s b join %s c on b.integer_test = c.integer_test where a.pk < b.pk)";
        String pointSql = "select * from %s a where bigint_test < 10";

        String failed = "doesn't exist";

        String[] hints = new String[] {
            "/*+TDDL:node('0')*/", "/*+TDDL:node('1')*/", "/*+TDDL:node('0','1')*/",
            "/*+TDDL:node('1','2','3')*/", "/*+TDDL:node=0*/"
        };

        // point select
        for (String nodeHint : hints) {
            JdbcUtil.executeSuccess(tddlConnection, nodeHint + String.format(pointSql, group2[1]));
            JdbcUtil.executeQueryFaied(tddlConnection, nodeHint + String.format(pointSql, group3[1]), failed);
            JdbcUtil.executeQueryFaied(tddlConnection, nodeHint + String.format(pointSql, group4[1]), failed);
            JdbcUtil.executeSuccess(tddlConnection, nodeHint + String.format(pointSql, group5[1]));
        }

        for (String nodeHint : hints) {
            JdbcUtil.executeSuccess(tddlConnection,
                nodeHint + String.format(joinSql, group2[1], group2[2], group2[3]));
            JdbcUtil.executeSuccess(tddlConnection,
                nodeHint + String.format(joinSql, group2[1], group5[2], group2[0]));
            JdbcUtil.executeQueryFaied(tddlConnection, nodeHint +
                String.format(joinSql, group1[2], group2[2], group2[0]), failed);

            JdbcUtil.executeSuccess(tddlConnection,
                nodeHint + String.format(subQuerySql, group2[1], group2[2], group2[3]));
            JdbcUtil.executeSuccess(tddlConnection,
                nodeHint + String.format(subQuerySql, group2[1], group5[2], group2[2]));
            JdbcUtil.executeQueryFaied(tddlConnection, nodeHint +
                String.format(subQuerySql, group1[2], group2[2], group2[0]), failed);
        }

        //test single table
        for (int i = 0; i < hints.length; i++) {
            switch (i) {
            case 0:
            case 4:
                JdbcUtil.executeSuccess(tddlConnection,
                    hints[i] + String.format(joinSql, group1[1], group1[2], group1[3]));
                JdbcUtil.executeSuccess(tddlConnection,
                    hints[i] + String.format(subQuerySql, group1[1], group1[2], group1[3]));
                JdbcUtil.executeSuccess(tddlConnection, hints[i] + String.format(pointSql, group1[3]));
                break;
            case 1:
            case 2:
            case 3:
                JdbcUtil.executeQueryFaied(tddlConnection,
                    hints[i] + String.format(joinSql, group1[1], group1[2], group1[2]), failed);
                JdbcUtil.executeQueryFaied(tddlConnection,
                    hints[i] + String.format(subQuerySql, group1[1], group1[2], group1[3]), failed);
                JdbcUtil.executeQueryFaied(tddlConnection, hints[i] + String.format(pointSql, group1[3]), failed);
                break;
            }
        }

        // test broadcast table
        for (String s : hints) {
            JdbcUtil.executeSuccess(tddlConnection,
                s + String.format(joinSql, group5[1], group5[2], group5[0]));
            JdbcUtil.executeSuccess(tddlConnection,
                s + String.format(subQuerySql, group5[1], group5[2], group5[3]));
            JdbcUtil.executeSuccess(tddlConnection, s + String.format(pointSql, group5[3]));
        }
    }

    @Test
    public void testRandomNodeHint() {
        String joinSql = "select * from %s a join %s b on a.pk = b.pk join %s c on a.integer_test = c.integer_test";
        String subQuerySql = "select * from %s a where a.integer_test not in "
            + "(select c.pk from %s b join %s c on b.integer_test = c.integer_test where a.pk < b.pk)";
        String pointSql = "select * from %s a where bigint_test < 10";

        String failed = "doesn't exist";

        String[] hints = new String[] {
            "/*+TDDL:random_node('0')*/",
            "/*+TDDL:random_node('1')*/",
            "/*+TDDL:random_node('0,1')*/",
            "/*+TDDL:random_node('1,2,3', 2)*/",
            "/*+TDDL:random_node()*/",
        };

        // point select
        for (String nodeHint : hints) {
            JdbcUtil.executeSuccess(tddlConnection, nodeHint + String.format(pointSql, group2[1]));
            JdbcUtil.executeQueryFaied(tddlConnection, nodeHint + String.format(pointSql, group3[1]), failed);
            JdbcUtil.executeQueryFaied(tddlConnection, nodeHint + String.format(pointSql, group4[1]), failed);
            JdbcUtil.executeSuccess(tddlConnection, nodeHint + String.format(pointSql, group5[1]));
        }

        for (String nodeHint : hints) {
            JdbcUtil.executeSuccess(tddlConnection,
                nodeHint + String.format(joinSql, group2[1], group2[2], group2[3]));
            JdbcUtil.executeSuccess(tddlConnection,
                nodeHint + String.format(joinSql, group2[1], group5[2], group2[0]));
            JdbcUtil.executeQueryFaied(tddlConnection, nodeHint +
                String.format(joinSql, group1[2], group2[2], group2[0]), failed);

            JdbcUtil.executeSuccess(tddlConnection,
                nodeHint + String.format(subQuerySql, group2[1], group2[2], group2[3]));
            JdbcUtil.executeSuccess(tddlConnection,
                nodeHint + String.format(subQuerySql, group2[1], group5[2], group2[2]));
            JdbcUtil.executeQueryFaied(tddlConnection, nodeHint +
                String.format(subQuerySql, group1[2], group2[2], group2[0]), failed);
        }

        //test single table
        for (int i = 0; i < hints.length; i++) {
            switch (i) {
            case 4:
                JdbcUtil.executeSuccess(tddlConnection,
                    hints[i] + String.format(joinSql, group1[1], group1[2], group1[3]));
                JdbcUtil.executeSuccess(tddlConnection,
                    hints[i] + String.format(subQuerySql, group1[1], group1[2], group1[3]));
                JdbcUtil.executeSuccess(tddlConnection, hints[i] + String.format(pointSql, group1[3]));
                break;
            case 0:
            case 1:
            case 2:
            case 3:
                JdbcUtil.executeQueryFaied(tddlConnection,
                    hints[i] + String.format(joinSql, group1[1], group1[2], group1[2]), failed);
                JdbcUtil.executeQueryFaied(tddlConnection,
                    hints[i] + String.format(subQuerySql, group1[1], group1[2], group1[3]), failed);
                JdbcUtil.executeQueryFaied(tddlConnection, hints[i] + String.format(pointSql, group1[3]), failed);
                break;
            }
        }

        // test broadcast table
        for (String s : hints) {
            JdbcUtil.executeSuccess(tddlConnection,
                s + String.format(joinSql, group5[1], group5[2], group5[0]));
            JdbcUtil.executeSuccess(tddlConnection,
                s + String.format(subQuerySql, group5[1], group5[2], group5[3]));
            JdbcUtil.executeSuccess(tddlConnection, s + String.format(pointSql, group5[3]));
        }
    }
}
