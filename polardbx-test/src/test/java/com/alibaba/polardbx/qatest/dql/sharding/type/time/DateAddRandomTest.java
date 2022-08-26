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

/**
 * DATE_ADD(date,INTERVAL expr unit)
 * DATE_SUB(date,INTERVAL expr unit)
 * TIMESTAMP_ADD(unit,interval,datetime_expr)
 */

public class DateAddRandomTest extends TimeTestBase {
    private String interval;

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(new String[][] {
            {"MICROSECOND"},
            {"SECOND"},
            {"MINUTE"},
            {"HOUR"},
            {"DAY"},
            {"MONTH"},
            {"YEAR"},
            {"QUARTER"},
            {"WEEK"}
        });
    }

    public DateAddRandomTest(String interval) {
        this.interval = interval;
    }

    private static final String FORMAT_1 = "SELECT DATE_ADD(? ,INTERVAL ? %s)";

    private static final int CHUNK_SIZE = 1 << 7;

    /**
     * for datetime string parameter
     */
    @Test
    public void testDateAdd() {
        String sql = String.format(FORMAT_1, interval);
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateDatetimeString(1)
            )
            .map(
                l -> ImmutableList.of(
                    l.get(0),
                    RandomTimeGenerator.R.nextInt(200) - 100))
            .forEach(
                param -> selectContentSameAssert(sql, param, mysqlConnection, tddlConnection)
            );
    }

    /**
     * for time type (0 fsp) parameter
     */
    @Test
    public void testDateAddTime0() {
        String sql = String.format("select concat(date_add(cast(? as time(0)), interval ? %s))", interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidTimeString(1)
            )
            .map(
                l -> ImmutableList.of(
                    l.get(0),
                    RandomTimeGenerator.R.nextInt(200) - 100))
            .forEach(
                param -> {
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for time type (3 fsp) parameter
     */
    @Test
    public void testDateAddTime3() {
        String sql = String.format("select concat(date_add(cast(? as time(3)), interval ? %s))", interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidTimeString(1)
            )
            .map(
                l -> ImmutableList.of(
                    l.get(0),
                    RandomTimeGenerator.R.nextInt(200) - 100))
            .forEach(
                param -> {
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for time type (6 fsp) parameter
     */
    @Test
    public void testDateAddTime6() {
        String sql = String.format("select concat(date_add(cast(? as time(6)), interval ? %s))", interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidTimeString(1)
            )
            .map(
                l -> ImmutableList.of(
                    l.get(0),
                    RandomTimeGenerator.R.nextInt(200) - 100))
            .forEach(
                param -> {
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for datetime type (0 fsp) parameter
     */
    @Test
    public void testDateAddDatetime0() {
        String sql = String.format("select concat(date_add(cast(? as datetime(0)), interval ? %s))", interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(1)
            )
            .map(
                l -> ImmutableList.of(
                    l.get(0),
                    RandomTimeGenerator.R.nextInt(200) - 100))
            .forEach(
                param -> {
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for datetime type (3 fsp) parameter
     */
    @Test
    public void testDateAddDatetime3() {
        String sql = String.format("select concat(date_add(cast(? as datetime(3)), interval ? %s))", interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(1)
            )
            .map(
                l -> ImmutableList.of(
                    l.get(0),
                    RandomTimeGenerator.R.nextInt(200) - 100))
            .forEach(
                param -> {
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for datetime type (6 fsp) parameter
     */
    @Test
    public void testDateAddDatetime6() {
        String sql = String.format("select concat(date_add(cast(? as datetime(6)), interval ? %s))", interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(1)
            )
            .map(
                l -> ImmutableList.of(
                    l.get(0),
                    RandomTimeGenerator.R.nextInt(200) - 100))
            .forEach(
                param -> {
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for date type parameter
     */
    @Test
    public void testDateAddDate() {
        String sql = String.format("select concat(date_add(cast(? as date), interval ? %s))", interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(1)
            )
            .map(
                l -> ImmutableList.of(
                    l.get(0),
                    RandomTimeGenerator.R.nextInt(200) - 100))
            .forEach(
                param -> {
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }
}
