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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.executor.vectorized.BaseProjectionTest;
import com.alibaba.polardbx.executor.vectorized.ColumnInput;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.Lists;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils.loadConfig;

@RunWith(Parameterized.class)
public class ComparisonBinaryOperatorsTest extends BaseProjectionTest {
    private static final Map<String, BiFunction<BigDecimal, BigDecimal, Boolean>> OPERATOR_TO_CMP =
        new HashMap<String, BiFunction<BigDecimal, BigDecimal, Boolean>>() {{
            put(">", ComparisonBinaryOperatorsTest::greater);
            put(">=", ComparisonBinaryOperatorsTest::greaterOrEqual);
            put("<", ComparisonBinaryOperatorsTest::less);
            put("<=", ComparisonBinaryOperatorsTest::lessOrEqual);
            put("=", ComparisonBinaryOperatorsTest::equal);
            put("<=>", ComparisonBinaryOperatorsTest::nullSafeEqual);
            put("!=", ComparisonBinaryOperatorsTest::notEqual);
            put("<>", ComparisonBinaryOperatorsTest::notEqual);
        }};

    private static final String CONFIG_COLUMN_PAIRS = "column_pairs";
    private static final List<Pair<String, String>> COLUMN_PAIRS = loadColumnPairs();

    public ComparisonBinaryOperatorsTest(String sql,
                                         List<ColumnInput> inputs,
                                         Block output, int[] selection) {
        super(sql, inputs, output, selection);
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        List<Object[]> result = new ArrayList<>(COLUMN_PAIRS.size() * COLUMN_PAIRS.size() * OPERATOR_TO_CMP.size());
        for (String operator : OPERATOR_TO_CMP.keySet()) {
            for (Pair<String, String> columnPair : COLUMN_PAIRS) {
                result.addAll(generateColCol(columnPair.getKey(), columnPair.getValue(), operator));
                result.addAll(generateColConst(columnPair.getKey(), columnPair.getValue(), operator));
            }
        }
        return result;
    }

    private static List<Object[]> generateColCol(String column1, String column2, String operator) {
        BiFunction<BigDecimal, BigDecimal, Boolean> cmp = OPERATOR_TO_CMP.get(operator);
        Random random = new Random(VectorizedExpressionTestUtils.SEED);
        List<Object[]> column1DataList = Lists.newArrayList(generateColumnInput(random, column1, true),
            generateColumnInput(random, column1, false));

        List<Object[]> column2DataList = Lists.newArrayList(generateColumnInput(random, column2, true),
            generateColumnInput(random, column2, false));

        int[] selection = createSelectionArray(SELECTION_LEN, TEST_ROW_COUNT, random);

        List<Object[]> results = new ArrayList<>(column1DataList.size() * column2DataList.size());
        for (Object[] column1Data : column1DataList) {
            for (Object[] column2Data : column2DataList) {
                results.add(new Object[] {
                    projection(String.format("%s %s %s", column1, operator, column2)),
                    Lists.newArrayList(columnInput(column1, column1Data), columnInput(column2, column2Data)),
                    output(DataTypes.LongType, getColColOutput(column1Data, column2Data, cmp)),
                    selection
                });
            }
        }
        return results;
    }

    private static List<Object[]> generateColConst(String column1, String column2, String operator) {
        BiFunction<BigDecimal, BigDecimal, Boolean> cmp = OPERATOR_TO_CMP.get(operator);
        Random random = new Random(VectorizedExpressionTestUtils.SEED);
        List<Object[]> column1DataList = Lists.newArrayList(
            generateColumnInput(random, column1, true),
            generateColumnInput(random, column1, false));

        DataType<?> dataType2 = dataTypeOfColumn(column2);
        List<Object> col2List = Lists.newArrayList(convert(random.nextDouble(), dataType2), null);
        List<Object> col2StrList = col2List.stream()
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

        List<Object[]> result = new ArrayList<>(column1DataList.size() * col2List.size() * 2);
        for (Object[] column1Data : column1DataList) {
            for (int i = 0; i < col2List.size(); i++) {
                result.add(new Object[] {
                    projection(String.format("%s %s %s", column1, operator, col2StrList.get(i))),
                    Lists.newArrayList(columnInput(column1, column1Data)),
                    output(DataTypes.LongType, getColConstOutput(column1Data, col2List.get(i), cmp, false)),
                    selection
                });
                result.add(new Object[] {
                    projection(String.format("%s %s %s", col2StrList.get(i), operator, column1)),
                    Lists.newArrayList(columnInput(column1, column1Data)),
                    output(DataTypes.LongType, getColConstOutput(column1Data, col2List.get(i), cmp, true)),
                    selection
                });
            }
        }

        return result;
    }

    private static List<Pair<String, String>> loadColumnPairs() {
        Map<String, Object> config = loadConfig(ComparisonBinaryOperatorsTest.class, Map.class);
        List<List<String>> pairs = (List<List<String>>) config.get(CONFIG_COLUMN_PAIRS);
        return pairs.stream()
            .map(p -> of(p.get(0), p.get(1)))
            .collect(Collectors.toList());
    }

    private static Object[] getColColOutput(Object[] column1Data, Object[] column2Data,
                                            BiFunction<BigDecimal, BigDecimal, Boolean> cmp) {
        Preconditions.checkArgument(column1Data.length == column2Data.length, "Input length should be equal!");
        Object[] output = new Object[column1Data.length];
        for (int i = 0; i < column1Data.length; i++) {
            BigDecimal left =
                (column1Data[i] != null) ? new BigDecimal(column1Data[i].toString(), MathContext.UNLIMITED) : null;
            BigDecimal right =
                (column2Data[i] != null) ? new BigDecimal(column2Data[i].toString(), MathContext.UNLIMITED) : null;
            output[i] = cmp.apply(left, right);
        }

        return output;
    }

    private static Object[] getColConstOutput(Object[] column1Data, Object col2,
                                              BiFunction<BigDecimal, BigDecimal, Boolean> cmp, boolean reverse) {
        Object[] result = new Object[column1Data.length];
        for (int i = 0; i < column1Data.length; i++) {
            BigDecimal col1 =
                (column1Data[i] == null) ? null : new BigDecimal(column1Data[i].toString(), MathContext.UNLIMITED);
            BigDecimal col2D = (col2 != null) ? new BigDecimal(col2.toString(), MathContext.UNLIMITED) : null;
            if (reverse) {
                result[i] = cmp.apply(col2D, col1);
            } else {
                result[i] = cmp.apply(col1, col2D);
            }
        }
        return result;
    }

    private static Boolean greater(BigDecimal d1, BigDecimal d2) {
        if ((d1 == null) || (d2 == null)) {
            return null;
        }

        return d1.compareTo(d2) > 0;
    }

    private static Boolean greaterOrEqual(BigDecimal d1, BigDecimal d2) {
        if ((d1 == null) || (d2 == null)) {
            return null;
        }

        return d1.compareTo(d2) >= 0;
    }

    private static Boolean equal(BigDecimal d1, BigDecimal d2) {
        if ((d1 == null) || (d2 == null)) {
            return null;
        }

        return d1.compareTo(d2) == 0;
    }

    private static Boolean nullSafeEqual(BigDecimal d1, BigDecimal d2) {
        if ((d1 == null) && (d2 == null)) {
            return true;
        }

        if ((d1 != null) && (d2 != null)) {
            return d1.compareTo(d2) == 0;
        }

        return false;
    }

    private static Boolean notEqual(BigDecimal d1, BigDecimal d2) {
        if ((d1 == null) || (d2 == null)) {
            return null;
        }

        return d1.compareTo(d2) != 0;
    }

    private static Boolean less(BigDecimal d1, BigDecimal d2) {
        if ((d1 == null) || (d2 == null)) {
            return null;
        }

        return d1.compareTo(d2) < 0;
    }

    private static Boolean lessOrEqual(BigDecimal d1, BigDecimal d2) {
        if ((d1 == null) || (d2 == null)) {
            return null;
        }

        return d1.compareTo(d2) <= 0;
    }
}