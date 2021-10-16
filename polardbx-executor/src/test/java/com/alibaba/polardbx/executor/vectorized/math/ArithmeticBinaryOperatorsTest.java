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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.annotation.Parameter;
import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.executor.utils.TestParameterUtil;
import com.alibaba.polardbx.executor.vectorized.BaseProjectionTest;
import com.alibaba.polardbx.executor.vectorized.ColumnInput;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionTestUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ArgumentInfo;
import com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignature;
import com.alibaba.polardbx.optimizer.core.datatype.Calculator;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.Lists;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
public class ArithmeticBinaryOperatorsTest extends BaseProjectionTest {
    private static final Map<String, String> OPERATOR_TO_CALC_METHOD_NAME = new HashMap<String, String>() {{
        put("+", "add");
        put("-", "sub");
        put("*", "multiply");
        put("/", "divide");
        put("%", "mod");
    }};

    private static final Map<String, List<Pair<String, String>>> COLUMN_PAIRS = loadColumnPairs();

    public ArithmeticBinaryOperatorsTest(String sql,
                                         List<ColumnInput> inputs,
                                         Block output, int[] selection) {
        super(sql, inputs, output, selection);
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters() {
        return TestParameterUtil.parameters(ArithmeticBinaryOperatorsTest.class);
    }

    @Parameter
    public static List<Object[]> generateColCol() throws Exception {
        List<Object[]> result = new ArrayList<>(COLUMN_PAIRS.size() * OPERATOR_TO_CALC_METHOD_NAME.size());
        for (String operator : OPERATOR_TO_CALC_METHOD_NAME.keySet()) {
            for (Pair<String, String> pair : COLUMN_PAIRS.get(OPERATOR_TO_CALC_METHOD_NAME.get(operator))) {
                result.addAll(generateColCol(pair.getKey(), pair.getValue(), operator));
            }
        }

        return result;
    }

    @Parameter
    public static List<Object[]> generateColConst() throws Exception {
        List<Object[]> result = new ArrayList<>(COLUMN_PAIRS.size() * OPERATOR_TO_CALC_METHOD_NAME.size());
        for (String operator : OPERATOR_TO_CALC_METHOD_NAME.keySet()) {
            for (Pair<String, String> pair : COLUMN_PAIRS.get(OPERATOR_TO_CALC_METHOD_NAME.get(operator))) {
                result.addAll(generateColConst(pair.getKey(), pair.getValue(), operator));
            }
        }

        return result;
    }

    private static List<Object[]> generateColCol(String column1, String column2, String operator)
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Random random = new Random(VectorizedExpressionTestUtils.SEED);
        DataType<?> dataType1 = dataTypeOfColumn(column1);
        List<Object[]> column1DataList = Lists.newArrayList(
            generateColumnInput(random, column1, false),
            generateColumnInput(random, column1, true));

        DataType<?> dataType2 = dataTypeOfColumn(column2);
        List<Object[]> column2DataList = Lists.newArrayList(
            generateColumnInput(random, column2, false),
            generateColumnInput(random, column2, true));

        int[] selection = createSelectionArray(SELECTION_LEN, TEST_ROW_COUNT, random);

        DataType<?> outputDataType = outputDataTypeOf(operator, dataType1, dataType2);

        List<Object[]> result = new ArrayList<>(column1DataList.size() * column2DataList.size());
        for (Object[] column1Data : column1DataList) {
            for (Object[] column2Data : column2DataList) {
                result.add(new Object[] {
                    projection(String.format("%s %s %s", column1, operator, column2)),
                    Lists.newArrayList(
                        columnInput(column1, column1Data),
                        columnInput(column2, column2Data)
                    ),
                    output(outputDataType, outputOf(column1Data, column2Data, outputDataType, operator)),
                    selection
                });
            }
        }

        return result;
    }

    private static List<Object[]> generateColConst(String column1, String column2, String operator)
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Random random = new Random(VectorizedExpressionTestUtils.SEED);
        DataType<?> dataType1 = dataTypeOfColumn(column1);
        List<Object[]> column1DataList = Lists.newArrayList(
            generateColumnInput(random, column1, false),
            generateColumnInput(random, column1, true));

        DataType<?> dataType2 = dataTypeOfColumn(column2);
        List<Pair<Object, String>> column2DataList = new ArrayList<>();
        Object column2RandomValue = convert(random.nextDouble() * 10, dataType2);
        if (!DataTypeUtil.equalsSemantically(dataType2, DataTypes.DoubleType) && !DataTypeUtil
            .equalsSemantically(dataType2, DataTypes.FloatType)) {
            column2DataList.add(of(0, "0"));
            column2DataList.add(of(column2RandomValue, String.format("%d", column2RandomValue)));
        }

        int[] selection = createSelectionArray(SELECTION_LEN, TEST_ROW_COUNT, random);

        DataType<?> outputDataType = outputDataTypeOf(operator, dataType1, dataType2);

        List<Object[]> result = new ArrayList<>(column1DataList.size() * column2DataList.size());
        for (Object[] column1Data : column1DataList) {
            for (Pair<Object, String> column2DataPair : column2DataList) {
                result.add(new Object[] {
                    projection(String.format("%s %s %s", column1, operator, column2DataPair.getValue())),
                    Lists.newArrayList(
                        columnInput(column1, column1Data)
                    ),
                    output(outputDataType, outputOf(column1Data, column2DataPair.getKey(), outputDataType, operator)),
                    selection
                });
                result.add(new Object[] {
                    projection(String.format("%s %s %s", column2DataPair.getValue(), operator, column1)),
                    Lists.newArrayList(
                        columnInput(column1, column1Data)
                    ),
                    output(outputDataType, outputOf(column2DataPair.getKey(), column1Data, outputDataType, operator)),
                    selection
                });
            }
        }

        return result;
    }

    private static DataType<?> outputDataTypeOf(String operator, DataType<?> dataType1, DataType<?> dataType2) {
        ArgumentInfo[] argumentInfos = new ArgumentInfo[2];
        argumentInfos[0] = new ArgumentInfo(dataType1, ArgumentKind.Variable);
        argumentInfos[1] = new ArgumentInfo(dataType2, ArgumentKind.Variable);
        return VectorizedExpressionTestUtils.outputDataTypeOf(new ExpressionSignature(operator, argumentInfos))
            .orElseThrow(() -> GeneralUtil.nestedException(
                String.format("Vectorized expression %s %s %s not found!", operator, dataType1, dataType2)));
    }

    private static Object[] outputOf(Object[] column1Data, Object[] column2Data, DataType<?> outputDataType,
                                     String operator)
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String calcMethodName = OPERATOR_TO_CALC_METHOD_NAME.get(operator);
        Calculator calc = outputDataType.getCalculator();
        Method method = Calculator.class.getMethod(calcMethodName, Object.class, Object.class);
        Object[] output = new Object[VectorizedExpressionTestUtils.TEST_ROW_COUNT];
        for (int i = 0; i < VectorizedExpressionTestUtils.TEST_ROW_COUNT; i++) {
            output[i] = method.invoke(calc, column1Data[i], column2Data[i]);
        }

        return output;
    }

    private static Object[] outputOf(Object[] column1Data, Object column2Data, DataType<?> outputDataType,
                                     String operator)
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String calcMethodName = OPERATOR_TO_CALC_METHOD_NAME.get(operator);
        Calculator calc = outputDataType.getCalculator();
        Method method = Calculator.class.getMethod(calcMethodName, Object.class, Object.class);
        Object[] output = new Object[VectorizedExpressionTestUtils.TEST_ROW_COUNT];
        for (int i = 0; i < VectorizedExpressionTestUtils.TEST_ROW_COUNT; i++) {
            output[i] = method.invoke(calc, column1Data[i], column2Data);
        }

        return output;
    }

    private static Object[] outputOf(Object column1Data, Object[] column2Data, DataType<?> outputDataType,
                                     String operator)
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String calcMethodName = OPERATOR_TO_CALC_METHOD_NAME.get(operator);
        Calculator calc = outputDataType.getCalculator();
        Method method = Calculator.class.getMethod(calcMethodName, Object.class, Object.class);
        Object[] output = new Object[VectorizedExpressionTestUtils.TEST_ROW_COUNT];
        for (int i = 0; i < VectorizedExpressionTestUtils.TEST_ROW_COUNT; i++) {
            output[i] = method.invoke(calc, column1Data, column2Data[i]);
        }

        return output;
    }

    private static Map<String, List<Pair<String, String>>> loadColumnPairs() {
        Map<String, Object> config = loadConfig(ArithmeticBinaryOperatorsTest.class, Map.class);
        Map<String, List<Pair<String, String>>> ret = new HashMap<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            List<Pair<String, String>> pairs = ((List<List<String>>) entry.getValue()).stream()
                .map(p -> of(p.get(0), p.get(1)))
                .collect(Collectors.toList());

            ret.put(entry.getKey(), pairs);
        }
        return ret;
    }
}

