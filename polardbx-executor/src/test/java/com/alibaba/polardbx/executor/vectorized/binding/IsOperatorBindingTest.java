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

package com.alibaba.polardbx.executor.vectorized.binding;

import com.alibaba.polardbx.executor.utils.EclipseParameterized;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlOperator;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_FALSE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_FALSE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_TRUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_UNKNOWN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_TRUE;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_UNKNOWN;

@Ignore
@RunWith(EclipseParameterized.class)
public class IsOperatorBindingTest extends BindingTestBase {
    private int index;
    private String op;
    private String field;

    private static final List<SqlOperator> IS_OPERATORS = ImmutableList.of(
        IS_NULL,
        IS_NOT_NULL,
        IS_TRUE,
        IS_FALSE,
        IS_UNKNOWN,
        IS_NOT_FALSE,
        IS_NOT_TRUE,
        IS_NOT_UNKNOWN
    );

    @Parameterized.Parameters(name = "{index}:index={0},op={1},field={2}")
    public static List<Object[]> prepare() {
        List<Object[]> list = new ArrayList<>();
        int i = 0;
        for (SqlOperator op : IS_OPERATORS) {
            for (String field : FIELDS) {
                list.add(new Object[] {i, op.getName(), field});
                i++;
            }
        }
        return list;
    }

    public IsOperatorBindingTest(int index, String op, String field) {
        this.index = index;
        this.op = op;
        this.field = field;
        loadPlans();
    }

    @Test
    public void testIs() {
        String sql = "select " + field + " " + op + " from test";
        String tree = testProject(sql).trees();
        compare(index, sql, tree);
    }
}
