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

package com.alibaba.polardbx.qatest.dql.sharding.type.time;

import com.alibaba.polardbx.common.utils.time.RandomTimeGenerator;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;


public class CastNumericTimeRandomTest extends TimeTestBase {
    private String fsp;

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(new String[][] {
            {"0"},
            {"1"},
            {"2"},
            {"3"},
            {"4"},
            {"5"},
            {"6"}
        });
    }

    public CastNumericTimeRandomTest(String fsp) {
        this.fsp = fsp;
    }

    public static final int NUM_OF_CAST = 128;
    public static String FORMAT_1;
    public static String FORMAT_2 = "select concat(cast(? as time(%s)), '')";
    public static String FORMAT_3 = "select concat(cast(? as date), '')";

    static {
        StringBuilder builder = new StringBuilder().append("select concat(");
        for (int i = 0; i < NUM_OF_CAST; i++) {
            builder.append("CAST(? AS datetime(%s))");
            if (i != NUM_OF_CAST - 1) {
                builder.append(", ");
            }
        }
        builder.append(")");
        FORMAT_1 = builder.toString();
    }

    @Test
    public void testRandomDatetimeDouble() {

        String sql = String.format("select concat(cast(? as datetime(%s)))", fsp);
        IntStream.range(0, TimeTestBase.DEFAULT_CHUNK_SIZE)
            .mapToObj(i -> RandomTimeGenerator.generateValidDatetimeNumeric(1))
            .forEach(l ->
                selectContentSameAssert(sql, l, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testRandomTime() {
        String sql = String.format("select concat(cast(? as time(%s)))", fsp);
        IntStream.range(0, TimeTestBase.DEFAULT_CHUNK_SIZE)
            .mapToObj(i -> RandomTimeGenerator.generateValidDatetimeNumeric(1))
            .forEach(l ->
                selectContentSameAssert(sql, l, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testRandomDate() {
        String sql = String.format("select concat(cast(? as date))");
        IntStream.range(0, TimeTestBase.DEFAULT_CHUNK_SIZE)
            .mapToObj(i -> RandomTimeGenerator.generateValidDatetimeNumeric(1))
            .forEach(l ->
                selectContentSameAssert(sql, l, mysqlConnection, tddlConnection)
            );
    }
}

