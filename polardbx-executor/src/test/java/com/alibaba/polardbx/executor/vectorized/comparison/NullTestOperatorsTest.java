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

package com.alibaba.polardbx.executor.vectorized.comparison;

import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.vectorized.BaseProjectionTest;
import com.alibaba.polardbx.executor.vectorized.ColumnInput;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.Lists;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.vectorized.ColumnInput.columnInput;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.SELECTION_LEN;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.TEST_ROW_COUNT;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.columnNames;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.createSelectionArray;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.generateColumnInput;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.getRandom;

@RunWith(Parameterized.class)
public class NullTestOperatorsTest extends BaseProjectionTest {
    private static final Map<String, Function<Object, Boolean>> FUNCTIONS =
        new HashMap<String, Function<Object, Boolean>>() {{
            put("%s IS NULL", NullTestOperatorsTest::isNull);
            put("ISNULL(%s)", NullTestOperatorsTest::isNull);
            put("%s IS NOT NULL", NullTestOperatorsTest::isNotNull);
        }};

    public NullTestOperatorsTest(String sql,
                                 List<ColumnInput> inputs,
                                 Block output, int[] selection) {
        super(sql, inputs, output, selection);
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> generate() {
        List<Object[]> result = new ArrayList<>(FUNCTIONS.size());
        for (String columnName : columnNames()) {
            for (String funcName : FUNCTIONS.keySet()) {
                result.addAll(generateParameters(columnName, funcName));
            }
        }

        return result;
    }

    private static List<Object[]> generateParameters(String columnName, String funcName) {
        Random random = getRandom();
        List<Object[]> inputs = Lists.newArrayList(
            generateColumnInput(random, columnName, true),
            generateColumnInput(random, columnName, false)
        );

        int[] selection = createSelectionArray(SELECTION_LEN, TEST_ROW_COUNT, random);

        return inputs.stream()
            .map(input -> new Object[] {
                projection(String.format(funcName, columnName)),
                Collections.singletonList(columnInput(columnName, input)),
                output(DataTypes.LongType, getOutput(input, funcName)),
                selection
            })
            .collect(Collectors.toList());
    }

    private static Object[] getOutput(Object[] input, String funcName) {
        Function<Object, Boolean> func = FUNCTIONS.get(funcName);
        return Arrays.stream(input)
            .map(func)
            .map(b -> b ? 1 : 0)
            .toArray(Object[]::new);
    }

    private static boolean isNull(Object obj) {
        return obj == null;
    }

    private static boolean isNotNull(Object obj) {
        return !isNull(obj);
    }
}