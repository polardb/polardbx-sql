/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dql.auto.hint;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class ScanHintTest extends AutoReadBaseTestCase {

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

    public ScanHintTest(String g1tb1, String g1tb2, String g1tb3, String g1tb4,
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
    public void testPointSelect() {
        final String pointSql = "select * from %s a where bigint_test < 10";

        final String failed = "no physical table";

        String[] pointSelectHints = new String[] {
            "/*+TDDL:SCAN()*/",
            "/*+TDDL:SCAN('%s')*/",
            "/*+TDDL:SCAN('%s', node='1')*/",
            "/*+TDDL:SCAN('%s', condition='pk = 3')*/",
            "/*+TDDL:SCAN('%s', condition='pk = rand() * 10')*/"
        };

        BiFunction<Integer, String, String> pointSelectSqlBuilder = (iHint, t) -> {
            final String scanHint = iHint == 0 ? pointSelectHints[iHint] : String.format(pointSelectHints[iHint], t);
            return scanHint + String.format(pointSql, t);
        };

        // point select
        for (int i = 0; i < pointSelectHints.length; i++) {
            JdbcUtil.executeSuccess(tddlConnection, pointSelectSqlBuilder.apply(i, group1[1]));
            JdbcUtil.executeSuccess(tddlConnection, pointSelectSqlBuilder.apply(i, group2[1]));
            JdbcUtil.executeSuccess(tddlConnection, pointSelectSqlBuilder.apply(i, group3[1]));
            JdbcUtil.executeSuccess(tddlConnection, pointSelectSqlBuilder.apply(i, group4[1]));
            JdbcUtil.executeSuccess(tddlConnection, pointSelectSqlBuilder.apply(i, group5[1]));
        }
    }

    @Test
    public void testMultiTable() {
        final String joinSql =
            "select * from %s a join %s b on a.pk = b.pk join %s c on a.integer_test = c.integer_test";
        final String subQuerySql = "select * from %s a where a.integer_test not in "
            + "(select c.pk from %s b join %s c on b.integer_test = c.integer_test where a.pk < b.pk)";

        final String failed = "no physical table";

        final String[] multiTableHints = new String[] {
            "/*+TDDL:SCAN()*/",
            "/*+TDDL:SCAN('%s, %s')*/",
            "/*+TDDL:SCAN('%s, %s, %s')*/",
            "/*+TDDL:SCAN('%s a, %s b, %s c', node='1')*/",
            "/*+TDDL:SCAN('%s a, %s b, %s c', condition='a.pk = 3 and a.pk = b.pk and c.pk = 3')*/",
            "/*+TDDL:SCAN('%s a, %s b, %s c', condition='a.pk = rand() * 10 and a.pk = b.pk and a.pk = c.pk')*/"
        };

        final BiFunction<Pair<Integer, List<String>>, Supplier<String>, String> multiTableQueryBuilder = (p, s) -> {
            final int iHint = p.getKey();
            final List<String> t = p.getValue();
            String scanHint;
            switch (iHint) {
            case 0:
                scanHint = multiTableHints[iHint];
                break;
            case 1:
                scanHint = String.format(multiTableHints[iHint], t.get(0), t.get(1));
                break;
            default:
                scanHint = String.format(multiTableHints[iHint], t.get(0), t.get(1), t.get(2));
            }
            return scanHint + String.format(s.get(), t.get(0), t.get(1), t.get(2));
        };

        final BiFunction<Integer, List<String>, String> joinBuilder =
            (iHint, t) -> multiTableQueryBuilder.apply(Pair.of(iHint, t), () -> joinSql);

        final BiFunction<Integer, List<String>, String> subqueryBuilder =
            (iHint, t) -> multiTableQueryBuilder.apply(Pair.of(iHint, t), () -> subQuerySql);

        // join
        for (int i = 0; i < multiTableHints.length; i++) {
            // single table
            JdbcUtil.executeSuccess(tddlConnection,
                joinBuilder.apply(i, Lists.newArrayList(group1[1], group1[2], group1[3])));
            // multi db one tb
            JdbcUtil.executeSuccess(tddlConnection,
                joinBuilder.apply(i, Lists.newArrayList(group2[1], group2[2], group2[3])));
            // multi db one tb + broadcast
            JdbcUtil.executeSuccess(tddlConnection,
                joinBuilder.apply(i, Lists.newArrayList(group2[1], group5[2], group2[0])));
            // multi db one tb + single table
            JdbcUtil.executeQuerySuccess(tddlConnection,
                joinBuilder.apply(i, Lists.newArrayList(group1[2], group2[2], group2[0])));
            // broadcast table
            JdbcUtil.executeSuccess(tddlConnection,
                joinBuilder.apply(i, Lists.newArrayList(group5[1], group5[2], group5[0])));
        }

        // subquery
        for (int i = 0; i < multiTableHints.length; i++) {
            // single table
            JdbcUtil.executeSuccess(tddlConnection,
                subqueryBuilder.apply(i, Lists.newArrayList(group1[1], group1[2], group1[3])));
            // multi db one tb
            JdbcUtil.executeSuccess(tddlConnection,
                subqueryBuilder.apply(i, Lists.newArrayList(group2[1], group2[2], group2[3])));
            // multi db one tb + broadcast
            JdbcUtil.executeSuccess(tddlConnection,
                subqueryBuilder.apply(i, Lists.newArrayList(group2[1], group5[2], group2[0])));
            // multi db one tb + single table
            JdbcUtil.executeSuccess(tddlConnection,
                subqueryBuilder.apply(i, Lists.newArrayList(group1[2], group2[2], group2[0])));
            // broadcast table
            JdbcUtil.executeSuccess(tddlConnection,
                subqueryBuilder.apply(i, Lists.newArrayList(group5[1], group5[2], group5[3])));
        }
    }
}
