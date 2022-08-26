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

public class BinaryArithmeticRandomTest extends NumericTestBase {

    final String col1;
    final String col2;
    final String op;
    final String ARITHMETIC_FORMAT;

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
    };

    private static final String[] OPS = {
        "/"
    };

    @Parameterized.Parameters(name = "{index}:col1={0},col2={1},op={3}")
    public static List<String[]> prepare() {
        // Cartesian product of {precision, scale} and {col}
        List<String[]> list = Arrays.stream(COLS)
            .map(
                col1 -> Arrays.stream(COLS).map(
                        col2 -> Arrays.stream(OPS).map(
                            op -> new String[] {col1, col2, op}
                        ).collect(Collectors.toList())
                    )
                    .flatMap(List::stream)
                    .collect(Collectors.toList())
            )
            .flatMap(List::stream)
            .collect(Collectors.toList());

        return list;
    }

    public BinaryArithmeticRandomTest(String col1, String col2, String op) {
        super(randomTableName("binary_num_test", 4));
        this.col1 = col1;
        this.col2 = col2;
        this.op = op;
        this.ARITHMETIC_FORMAT =
            "/*+TDDL:ENABLE_PUSH_PROJECT=false*/select %s, %s, %s %s %s from " + TEST_TABLE;
    }

    @Test
    public void testColCol() {
        Supplier<Object> generator1 = getGenerator(col1);
        Supplier<Object> generator2 = getGenerator(col2, false);

        // generate insert value & do insert
        List<Object[]> paramList = IntStream.range(0, BATCH_SIZE)
            .mapToObj(i -> new Object[] {generator1.get(), generator2.get()})
            .collect(Collectors.toList());

        insertData(paramList, col1, col2);

        // select cast(col as decimal(precision, scale)) from table
        String sql = String.format(ARITHMETIC_FORMAT, col1, col2, col1, op, col2);
//        System.out.println(sql);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testColConst() {
        Supplier<Object> generator1 = getGenerator(col1);
        Supplier<Object> generator2 = getGenerator(col2, false);

        // generate insert value & do insert
        List<Object> paramList = IntStream.range(0, BATCH_SIZE)
            .mapToObj(i -> generator1.get())
            .collect(Collectors.toList());

        insertData(paramList, col1);

        Object constant = generator2.get();
        // select cast(col as decimal(precision, scale)) from table
        String sql = String.format(ARITHMETIC_FORMAT, col1, constant, col1, op, constant);
//        System.out.println(sql);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testConstCol() {
        Supplier<Object> generator1 = getGenerator(col1);
        Supplier<Object> generator2 = getGenerator(col2, false);

        // generate insert value & do insert
        List<Object> paramList = IntStream.range(0, BATCH_SIZE)
            .mapToObj(i -> generator2.get())
            .collect(Collectors.toList());

        insertData(paramList, col2);

        Object constant = generator1.get();
        // select cast(col as decimal(precision, scale)) from table
        String sql = String.format(ARITHMETIC_FORMAT, constant, col2, constant, op, col2);
//        System.out.println(sql);
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}
