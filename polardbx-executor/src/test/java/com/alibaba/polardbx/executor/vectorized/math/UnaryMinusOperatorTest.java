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

package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.executor.vectorized.BaseProjectionTest;
import com.alibaba.polardbx.executor.vectorized.ColumnInput;
import com.alibaba.polardbx.optimizer.core.datatype.Calculator;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.Lists;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.alibaba.polardbx.executor.vectorized.ColumnInput.columnInput;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.SEED;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.SELECTION_LEN;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.TEST_ROW_COUNT;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.createSelectionArray;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.dataTypeOfColumn;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.generateColumnInput;


@RunWith(Parameterized.class)
public class UnaryMinusOperatorTest extends BaseProjectionTest {
    private static final List<String> COLUMN_NAMES = Lists.newArrayList(
        "test_double",
        "test_float",
        "test_utinyint",
        "test_tinyint",
        "test_smallint",
        "test_usmallint",
        "test_mediumint",
        "test_umediumint",
        "test_integer",
        "test_uinteger",
        "test_bigint"
    );

    public UnaryMinusOperatorTest(String sql,
                                  List<ColumnInput> inputs,
                                  Block output, int[] selection) {
        super(sql, inputs, output, selection);
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        List<Object[]> result = new ArrayList<>(COLUMN_NAMES.size() * 2);
        for (String columnName : COLUMN_NAMES) {
            result.addAll(generate(columnName));
        }

        return result;
    }

    private static List<Object[]> generate(String columnName) {
        Random random = new Random(SEED);
        DataType<?> dataType = dataTypeOfColumn(columnName);
        Object[] columnDataWithoutNull = generateColumnInput(random, columnName, false);
        Object[] columnDataWithNull = generateColumnInput(random, columnName, true);

        int[] selection = createSelectionArray(SELECTION_LEN, TEST_ROW_COUNT, random);

        DataType<?> outputDataType = DataTypeUtil.isUnderLongType(dataType)
            ? DataTypes.LongType : DataTypes.DoubleType;

        Object[] outputWithoutNull = calculate(outputDataType, columnDataWithoutNull);
        Object[] outputWithNull = calculate(outputDataType, columnDataWithNull);

        return Lists.newArrayList(
            new Object[] {
                projection(String.format("-%s", columnName)),
                Lists.newArrayList(columnInput(columnName, columnDataWithoutNull)),
                output(outputDataType, outputWithoutNull),
                selection
            },
            new Object[] {
                projection(String.format("-%s", columnName)),
                Lists.newArrayList(columnInput(columnName, columnDataWithNull)),
                output(outputDataType, outputWithNull),
                selection
            });
    }

    private static Object[] calculate(DataType<?> dataType, Object[] input) {
        Object[] output = new Object[input.length];
        Calculator calc = dataType.getCalculator();
        for (int i = 0; i < input.length; i++) {
            output[i] = calc.multiply(input[i], -1);
        }

        return output;
    }
}