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

package com.alibaba.polardbx.qatest.dql.sharding.type.numeric;

import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * Test All Fields for CAST(column as DECIMAL(precision, scale)).
 * <p>
 * The parameters is Cartesian product of {precision, scale} X {col name}
 * And we have random value generator for specified column name.
 */

public class CastDecimalRandomTest extends NumericTestBase {
    private final String CAST_DECIMAL_FORMAT;

    final String precision;
    final String scale;
    final String col;

    protected static final String[] COLS = new String[] {
        DECIMAL_TEST_LOW,
        DECIMAL_TEST_HIGH,
        TINYINT_TEST,
        UTINYINT_TEST,
        SMALLINT_TEST,
        USMALLINT_TEST,
        MEDIUMINT_TEST,
        UMEDIUMINT_TEST,
        INT_TEST,
        UINT_TEST,
        BIGINT_TEST,
//        UBIGINT_TEST,
        VARCHAR_TEST,
        CHAR_TEST,
        LONG_NUMBER_TEST,
    };

    private static final String[][] PRECISION_AND_SCALES = new String[][] {
        {"4", "0"},
        {"15", "7"},
        {"65", "30"}
    };

    @Parameterized.Parameters(name = "{index}:precision={0},scale={1},col={2}")
    public static List<String[]> prepare() {
        // Cartesian product of {precision, scale} and {col}
        return Arrays.stream(COLS)
            .map(
                col -> Arrays.stream(PRECISION_AND_SCALES).map(
                    precisionAndScale -> new String[] {precisionAndScale[0], precisionAndScale[1], col}
                ).collect(Collectors.toList()))
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }

    public CastDecimalRandomTest(String precision, String scale, String col) {
        super(randomTableName("decimal_num_test", 4));
        this.precision = precision;
        this.scale = scale;
        this.col = col;
        this.CAST_DECIMAL_FORMAT =
            "/*+TDDL:ENABLE_PUSH_PROJECT=false*/select %s, cast(%s as decimal(%s, %s)) from " + TEST_TABLE;
    }

    @Test
    public void testAllField() {
        // Get random value generator by column name.
        Supplier<Object> generator = getGenerator(col);

        // generate insert value & do insert
        List<Object> paramList = IntStream.range(0, BATCH_SIZE)
            .mapToObj(i -> generator.get())
            .collect(Collectors.toList());

        insertData(paramList, col);

        // select cast(col as decimal(precision, scale)) from table
        String sql = String.format(CAST_DECIMAL_FORMAT, col, col, precision, scale);
//        System.out.println(sql);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
