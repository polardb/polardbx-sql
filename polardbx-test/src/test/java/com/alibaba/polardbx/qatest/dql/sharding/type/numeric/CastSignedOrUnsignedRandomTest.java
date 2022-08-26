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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@Ignore("The online test cannot print unsigned integer value.")

public class CastSignedOrUnsignedRandomTest extends NumericTestBase {
    private final String CAST_SIGNED_FORMAT;

    private final String CAST_UNSIGNED_FORMAT;

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
        UBIGINT_TEST,
        VARCHAR_TEST,
        CHAR_TEST,
        LONG_NUMBER_TEST,
    };

    final String col;

    @Parameterized.Parameters(name = "{index}:col={0}")
    public static List<String[]> prepare() {
        return Arrays.stream(COLS)
            .map(col -> new String[] {col})
            .collect(Collectors.toList());
    }

    public CastSignedOrUnsignedRandomTest(String col) {
        super(randomTableName("cast_num_test", 4));
        this.col = col;
        this.CAST_SIGNED_FORMAT =
            "/*+TDDL:ENABLE_PUSH_PROJECT=false*/select %s, cast(%s as signed) from " + TEST_TABLE;

        this.CAST_UNSIGNED_FORMAT =
            "/*+TDDL:ENABLE_PUSH_PROJECT=false*/select %s, cast(%s as unsigned) from " + TEST_TABLE;
    }

    @Test
    public void testAllFieldForSigned() {
        // Get random value generator by column name.
        Supplier<Object> generator = getGenerator(col);

        // generate insert value & do insert
        List<Object> paramList = IntStream.range(0, BATCH_SIZE)
            .mapToObj(i -> generator.get())
            .collect(Collectors.toList());

        insertData(paramList, col);

        // select cast(col as decimal(precision, scale)) from table
        String sql = String.format(CAST_SIGNED_FORMAT, col, col);
//        System.out.println(sql);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testAllFieldForUnsigned() {
        // Get random value generator by column name.
        Supplier<Object> generator = getGenerator(col);

        // generate insert value & do insert
        List<Object> paramList = IntStream.range(0, BATCH_SIZE)
            .mapToObj(i -> generator.get())
            .collect(Collectors.toList());

        insertData(paramList, col);

        // select cast(col as decimal(precision, scale)) from table
        String sql = String.format(CAST_UNSIGNED_FORMAT, col, col);
//        System.out.println(sql);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
