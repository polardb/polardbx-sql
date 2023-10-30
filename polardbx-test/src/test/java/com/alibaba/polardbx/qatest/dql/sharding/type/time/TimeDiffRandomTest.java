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
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;


public class TimeDiffRandomTest extends TimeTestBase {
    private static final int CHUNK_SIZE = 1 << 7;
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

    public TimeDiffRandomTest(String fsp) {
        this.fsp = fsp;
    }

    @Test
    public void testRandomDatetime() {
        String format = "select concat(timediff(cast(? as datetime(%s)), cast(? as datetime(%s))))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> ImmutableList.of(
                    RandomTimeGenerator.generateValidDatetimeString(1).get(0),
                    RandomTimeGenerator.generateValidDatetimeString(1).get(0)
                )
            )
            .forEach(
                param -> {
                    String sql = String.format(format, fsp, RandomTimeGenerator.R.nextInt(7));
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }

    @Test
    public void testRandomTime() {
        String format = "select concat(timediff(cast(? as time(%s)), cast(? as time(%s))))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> ImmutableList.of(
                    RandomTimeGenerator.generateValidTimeString(1).get(0),
                    RandomTimeGenerator.generateValidTimeString(1).get(0)
                )
            )
            .forEach(
                param -> {
                    String sql = String.format(format, fsp, RandomTimeGenerator.R.nextInt(7));
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }

    @Test
    public void testRandomDate() {
        String format = "select concat(timediff(cast(? as date), cast(? as date)))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> ImmutableList.of(
                    RandomTimeGenerator.generateValidDatetimeString(1).get(0),
                    RandomTimeGenerator.generateValidDatetimeString(1).get(0)
                )
            )
            .forEach(
                param -> {
                    String sql = String.format(format);
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }

    @Test
    public void testRandomDateAndDatetime() {
        String format = "select concat(subtime(cast(? as date), cast(? as datetime(%s))))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> ImmutableList.of(
                    RandomTimeGenerator.generateValidDatetimeString(1).get(0),
                    RandomTimeGenerator.generateValidDatetimeString(1).get(0)
                )
            )
            .forEach(
                param -> {
                    String sql = String.format(format, fsp);
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }

    @Test
    public void testRandomDatetimeAndDate() {
        String format = "select concat(subtime(cast(? as datetime(%s)), cast(? as date)))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> ImmutableList.of(
                    RandomTimeGenerator.generateValidDatetimeString(1).get(0),
                    RandomTimeGenerator.generateValidDatetimeString(1).get(0)
                )
            )
            .forEach(
                param -> {
                    String sql = String.format(format, fsp);
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }
}
