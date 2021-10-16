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

package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignature;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

public class VectorizedExpressionTestUtils {
    public static final int SEED = 1000;
    public static final int SELECTION_LEN = 3;
    public static final int TEST_ROW_COUNT = 5;

    private static final Map<String, DataType<?>> COLUMN_NAME_TO_DATA_TYPE = new HashMap<String, DataType<?>>() {{
        put("test_double", DataTypes.DoubleType);
        put("test_double2", DataTypes.DoubleType);
        put("test_float", DataTypes.FloatType);
        put("test_float2", DataTypes.FloatType);
        put("test_tinyint", DataTypes.TinyIntType);
        put("test_tinyint2", DataTypes.TinyIntType);
        put("test_utinyint", DataTypes.UTinyIntType);
        put("test_utinyint2", DataTypes.UTinyIntType);
        put("test_smallint", DataTypes.SmallIntType);
        put("test_smallint2", DataTypes.SmallIntType);
        put("test_usmallint", DataTypes.USmallIntType);
        put("test_usmallint2", DataTypes.USmallIntType);
        put("test_mediumint", DataTypes.MediumIntType);
        put("test_mediumint2", DataTypes.MediumIntType);
        put("test_umediumint", DataTypes.UMediumIntType);
        put("test_umediumint2", DataTypes.UMediumIntType);
        put("test_integer", DataTypes.IntegerType);
        put("test_integer2", DataTypes.IntegerType);
        put("test_uinteger", DataTypes.UIntegerType);
        put("test_uinteger2", DataTypes.UIntegerType);
        put("test_bigint", DataTypes.LongType);
        put("test_bigint2", DataTypes.LongType);
    }};

    public static Number convert(Number source, DataType<?> targetType) {
        Class<?> klass = targetType.getDataClass();
        if (klass == Byte.class) {
            return source.byteValue();
        } else if (klass == Short.class) {
            return source.shortValue();
        } else if (klass == Integer.class) {
            return source.intValue();
        } else if (klass == Long.class) {
            return source.longValue();
        } else if (klass == Float.class) {
            return source.floatValue();
        } else if (klass == Double.class) {
            return source.doubleValue();
        } else {
            throw new IllegalArgumentException("Unable to convert number to " + targetType);
        }
    }

    public static DataType<?> dataTypeOfColumn(String columnName) {
        return Optional.ofNullable(COLUMN_NAME_TO_DATA_TYPE.get(columnName))
            .orElseThrow(() -> GeneralUtil.nestedException("Column " + columnName + " does not exist!"));
    }

    public static Optional<DataType<?>> outputDataTypeOf(ExpressionSignature signature) {
        return VectorizedExpressionRegistry.builderConstructorOf(signature)
            .map(c -> c.build(0, new VectorizedExpression[0]))
            .map(VectorizedExpression::getOutputDataType);
    }

    public static Collection<String> columnNames() {
        return COLUMN_NAME_TO_DATA_TYPE.keySet();
    }

    public static <T> T loadConfig(Class<?> configHolder, Class<T> targetClass) {
        Yaml yaml = new Yaml();

        try (FileInputStream in = new FileInputStream(
            configHolder.getResource(configHolder.getSimpleName() + ".yml").getPath())) {
            return targetClass.cast(yaml.load(in));
        } catch (IOException e) {
            throw GeneralUtil.nestedException("Failed to load config!", e);
        }
    }

    public static Object[] generateColumnInput(Random random, String columnName, boolean withNull) {
        DataType<?> dataType = dataTypeOfColumn(columnName);
        Object[] columnData = new Object[TEST_ROW_COUNT];

        if (withNull) {
            for (int i = 0; i < TEST_ROW_COUNT; i++) {
                if (random.nextBoolean()) {
                    columnData[i] = convert(random.nextDouble() * 10, dataType);
                } else {
                    columnData[i] = null;
                }
            }
        } else {
            for (int i = 0; i < TEST_ROW_COUNT; i++) {
                columnData[i] = convert(random.nextDouble() * 10, dataType);
            }
        }

        return columnData;
    }

    public static Optional<BigDecimal> toBigDecimal(Object object) {
        return Optional.ofNullable(object)
            .map(v -> new BigDecimal(v.toString(), MathContext.UNLIMITED));
    }

    public static Random getRandom() {
        return new Random(SEED);
    }

    public static int[] createSelectionArray(int selectionCount, int rowCount, Random random) {
        int[] selection = new int[selectionCount];
        boolean[] bitmap = new boolean[rowCount];
        for (int i = 0; i < selection.length; i++) {
            int next = random.nextInt(rowCount);
            while (bitmap[next]) {
                next = random.nextInt(rowCount);
            }
            bitmap[next] = true;
            selection[i] = next;
        }

        Arrays.sort(selection);
        return selection;
    }
}