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

package com.alibaba.polardbx.executor.vectorized.logical;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.vectorized.BaseProjectionTest;
import com.alibaba.polardbx.executor.vectorized.ColumnInput;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.utils.Pair.of;
import static com.alibaba.polardbx.executor.vectorized.ColumnInput.columnInput;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.SELECTION_LEN;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.TEST_ROW_COUNT;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.convert;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.createSelectionArray;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.dataTypeOfColumn;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.generateColumnInput;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.getRandom;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.loadConfig;
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.toBigDecimal;

@RunWith(Parameterized.class)
public class LogicalBinaryOperatorsTest extends BaseProjectionTest {
    private static final Map<String, BiFunction<Object, Object, Object>> FUNCTION_MAP =
        new HashMap<String, BiFunction<Object, Object, Object>>() {{
            put("AND", LogicalBinaryOperatorsTest::and);
            put("OR", LogicalBinaryOperatorsTest::or);
            put("XOR", LogicalBinaryOperatorsTest::xor);
        }};

    private static final String CONFIG_COLUMN_PAIRS = "column_pairs";
    private static final List<Pair<String, String>> COLUMN_PAIRS = loadColumnPairs();

    public LogicalBinaryOperatorsTest(String sql,
                                      List<ColumnInput> inputs,
                                      Block output, int[] selection) {
        super(sql, inputs, output, selection);
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> generate() {
        List<Object[]> result = new ArrayList<>(COLUMN_PAIRS.size() * COLUMN_PAIRS.size() * FUNCTION_MAP.size());

        for (String operator : FUNCTION_MAP.keySet()) {
            for (Pair<String, String> columnPair : COLUMN_PAIRS) {
                result.addAll(generateColCol(columnPair.getKey(), columnPair.getValue(), operator));
                result.addAll(generateColConst(columnPair.getKey(), columnPair.getValue(), operator));
            }
        }
        return result;
    }

    protected static List<Object[]> generateColCol(String col1Name, String col2Name, String operator) {
        return generateColCol(col1Name, col2Name, operator, true);
    }

    protected static List<Object[]> generateColCol(String col1Name, String col2Name, String operator,
                                                   boolean withNull) {
        Random random = getRandom();
        List<Object[]> col1DataList;
        if (withNull) {
            col1DataList = Lists.newArrayList(
                generateColumnInput(random, col1Name, true),
                generateColumnInput(random, col1Name, false));
        } else {
            col1DataList = Lists.newArrayList(
                generateColumnInput(random, col1Name, false),
                generateColumnInput(random, col1Name, false));
        }

        List<Object[]> col2DataList;
        if (withNull) {
            col2DataList = Lists.newArrayList(
                generateColumnInput(random, col2Name, true),
                generateColumnInput(random, col2Name, false));
        } else {
            col2DataList = Lists.newArrayList(
                generateColumnInput(random, col2Name, false),
                generateColumnInput(random, col2Name, false));
        }

        int[] selection = createSelectionArray(SELECTION_LEN, TEST_ROW_COUNT, random);

        List<Object[]> result = new ArrayList<>(col1DataList.size() * col2DataList.size());

        for (Object[] col1Data : col1DataList) {
            for (Object[] col2Data : col2DataList) {
                result.add(new Object[] {
                    projection(String.format("%s %s %s", col1Name, operator, col2Name)),
                    Lists.newArrayList(columnInput(col1Name, col1Data), columnInput(col2Name, col2Data)),
                    output(DataTypes.LongType, calcColCol(col1Data, col2Data, operator)),
                    selection
                });
            }
        }

        return result;
    }

    protected static List<Object[]> generateColConst(String col1Name, String col2Name, String operator) {
        Random random = getRandom();
        DataType<?> dataType1 = dataTypeOfColumn(col1Name);
        List<Object[]> col1DataList = Lists.newArrayList(
            generateColumnInput(random, col1Name, true),
            generateColumnInput(random, col1Name, false));

        DataType<?> dataType2 = dataTypeOfColumn(col2Name);
        List<Object> col2DataList;
        if (DataTypeUtil.equalsSemantically(dataType1, DataTypes.DoubleType) || DataTypeUtil
            .equalsSemantically(dataType1, DataTypes.FloatType)) {
            col2DataList = Lists.newArrayList(convert(random.nextDouble(), dataType2));
        } else {
            col2DataList = Lists.newArrayList(convert(random.nextDouble(), dataType2), null);
        }

        List<String> col2StrList = col2DataList.stream()
            .map(d -> {
                if (d == null) {
                    return "NULL";
                }
                if (DataTypeUtil.equalsSemantically(dataType2, DataTypes.DoubleType) || DataTypeUtil
                    .equalsSemantically(dataType2, DataTypes.FloatType)) {
                    return String.format("%E", d);
                } else {
                    return String.format("%d", d);
                }
            }).collect(Collectors.toList());

        int[] selection = createSelectionArray(SELECTION_LEN, TEST_ROW_COUNT, random);

        List<Object[]> result = new ArrayList<>(col1DataList.size() * col2DataList.size() * 2);

        for (Object[] col1Data : col1DataList) {
            for (int i = 0; i < col2DataList.size(); i++) {
                Object col2Data = col2DataList.get(i);
                String col2Str = col2StrList.get(i);
                result.add(new Object[] {
                    projection(String.format("%s %s %s", col1Name, operator, col2Str)),
                    Lists.newArrayList(columnInput(col1Name, col1Data)),
                    output(DataTypes.LongType, calcColConst(col1Data, col2Data, operator, false)),
                    selection
                });

                result.add(new Object[] {
                    projection(String.format("%s %s %s", col2Str, operator, col1Name)),
                    Lists.newArrayList(columnInput(col1Name, col1Data)),
                    output(DataTypes.LongType, calcColConst(col1Data, col2Data, operator, true)),
                    selection
                });
            }

        }

        return result;
    }

    private static Object[] calcColCol(Object[] col1Data, Object[] col2Data, String operator) {
        Preconditions.checkArgument(col1Data.length == col2Data.length);
        BiFunction<Object, Object, Object> func = FUNCTION_MAP.get(operator);

        Object[] result = new Object[col1Data.length];
        for (int i = 0; i < col1Data.length; i++) {
            result[i] = func.apply(col1Data[i], col2Data[i]);
        }

        return result;
    }

    private static Object[] calcColConst(Object[] col1Data, Object col2Data, String operator, boolean reverse) {
        BiFunction<Object, Object, Object> func = FUNCTION_MAP.get(operator);

        Object[] result = new Object[col1Data.length];
        for (int i = 0; i < col1Data.length; i++) {
            if (reverse) {
                result[i] = func.apply(col2Data, col1Data[i]);
            } else {
                result[i] = func.apply(col1Data[i], col2Data);
            }
        }

        return result;
    }

    protected static Object and(Object obj1, Object obj2) {
        Optional<Boolean> b1 = toBigDecimal(obj1)
            .map(o -> o.compareTo(BigDecimal.ZERO) != 0);

        Optional<Boolean> b2 = toBigDecimal(obj2)
            .map(o -> o.compareTo(BigDecimal.ZERO) != 0);

        if (b1.isPresent() && b2.isPresent()) {
            return b1.get() && b2.get();
        } else if (b1.isPresent()) {
            if (b1.get()) {
                return null;
            } else {
                return false;
            }
        } else if (b2.isPresent()) {
            if (b2.get()) {
                return null;
            } else {
                return false;
            }
        } else {
            return null;
        }
    }

    protected static Object or(Object obj1, Object obj2) {
        Optional<Boolean> b1 = toBigDecimal(obj1)
            .map(o -> o.compareTo(BigDecimal.ZERO) != 0);

        Optional<Boolean> b2 = toBigDecimal(obj2)
            .map(o -> o.compareTo(BigDecimal.ZERO) != 0);

        if (b1.isPresent() && b2.isPresent()) {
            return b1.get() || b2.get();
        } else if (b1.isPresent()) {
            if (b1.get()) {
                return true;
            } else {
                return null;
            }
        } else if (b2.isPresent()) {
            if (b2.get()) {
                return true;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private static Object xor(Object obj1, Object obj2) {
        Optional<Boolean> b1 = toBigDecimal(obj1)
            .map(o -> o.compareTo(BigDecimal.ZERO) != 0);

        Optional<Boolean> b2 = toBigDecimal(obj2)
            .map(o -> o.compareTo(BigDecimal.ZERO) != 0);

        if (b1.isPresent() && b2.isPresent()) {
            return b1.get() ^ b2.get();
        } else {
            return null;
        }
    }

    private static List<Pair<String, String>> loadColumnPairs() {
        Map<String, Object> config = loadConfig(LogicalBinaryOperatorsTest.class, Map.class);
        List<List<String>> pairs = (List<List<String>>) config.get(CONFIG_COLUMN_PAIRS);
        return pairs.stream()
            .map(p -> of(p.get(0), p.get(1)))
            .collect(Collectors.toList());
    }
}
