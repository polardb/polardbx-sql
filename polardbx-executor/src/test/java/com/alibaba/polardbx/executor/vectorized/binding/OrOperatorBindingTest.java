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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

/**
 * test [filter mode] LOGICAL_OP [filter mode]
 * <p>
 * test [filter mode] LOGICAL_OP [project mode(output=BIGINT[])]
 * <p>
 * test [project mode (output=BIGINT[])] LOGICAL_OP [filter mode]
 * <p>
 * test [filter mode] LOGICAL_OP [input refs / literal / dynamic param]
 * <p>
 * test [input refs / literal / dynamic param] LOGICAL_OP [filter mode]
 */
@Ignore
@RunWith(EclipseParameterized.class)
public class OrOperatorBindingTest extends BindingTestBase {

    private int index;
    private String field1;
    private String field2;

    public OrOperatorBindingTest(int index, String field1, String field2) {
        this.index = index;
        this.field1 = field1;
        this.field2 = field2;
        loadPlans();
    }

    @Parameterized.Parameters(name = "{index}:index={0},field1={1},field2={2}")
    public static List<Object[]> prepare() {
        List<Object[]> list = new ArrayList<>();
        int i = 0;
        for (String field1 : VECTORIZED_FIELDS) {
            for (String field2 : VECTORIZED_FIELDS) {
                list.add(new Object[] {i, field1, field2});
                i++;
            }
        }
        return list;
    }

    @Test
    public void test() {
        String expr = field1 + " > " + field2;

        String sql = "select * from test where " + expr + " or integer_test < 1";
        String tree = testFilter(sql).trees();
        compare(index, sql, tree);
    }
}

