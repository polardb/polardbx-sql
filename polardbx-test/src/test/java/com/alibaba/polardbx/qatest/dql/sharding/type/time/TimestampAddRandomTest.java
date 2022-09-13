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

public class TimestampAddRandomTest extends TimeTestBase {
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

    public TimestampAddRandomTest(String interval) {
        this.interval = interval;
    }

    /**
     * for datetime string parameter
     */
    @Test
    public void testTimestampAdd() {
        // TIMESTAMPADD(unit,interval,datetime_expr)
        String format = "select concat(timestampadd(%s, %s, ?))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(1)
            )
            .forEach(
                param -> {
                    // timestampadd interval_value cannot be parameterized.
                    // don't know why.
                    String sql = String.format(
                        format,
                        interval,
                        RandomTimeGenerator.R.nextInt(200) - 100);
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for date type parameter
     */
    @Test
    public void testTimestampAddDate() {
        // TIMESTAMPADD(unit,interval,datetime_expr)
        String format = "select concat(timestampadd(%s, %s, cast(? as date)))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(1)
            )
            .forEach(
                param -> {
                    // timestampadd interval_value cannot be parameterized.
                    // don't know why.
                    String sql = String.format(
                        format,
                        interval,
                        RandomTimeGenerator.R.nextInt(200) - 100);
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for datetime type (fsp 0) parameter
     */
    @Test
    public void testTimestampAddDatetime0() {
        // TIMESTAMPADD(unit,interval,datetime_expr)
        String format = "select concat(timestampadd(%s, %s, cast(? as datetime(0))))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(1)
            )
            .forEach(
                param -> {
                    // timestampadd interval_value cannot be parameterized.
                    // don't know why.
                    String sql = String.format(
                        format,
                        interval,
                        RandomTimeGenerator.R.nextInt(200) - 100);
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for datetime type (fsp 3) parameter
     */
    @Test
    public void testTimestampAddDatetime3() {
        // TIMESTAMPADD(unit,interval,datetime_expr)
        String format = "select concat(timestampadd(%s, %s, cast(? as datetime(3))))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(1)
            )
            .forEach(
                param -> {
                    // timestampadd interval_value cannot be parameterized.
                    // don't know why.
                    String sql = String.format(
                        format,
                        interval,
                        RandomTimeGenerator.R.nextInt(200) - 100);
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for datetime type (fsp 0) parameter
     */
    @Test
    public void testTimestampAddDatetime6() {
        // TIMESTAMPADD(unit,interval,datetime_expr)
        String format = "select concat(timestampadd(%s, %s, cast(? as datetime(6))))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(1)
            )
            .forEach(
                param -> {
                    // timestampadd interval_value cannot be parameterized.
                    // don't know why.
                    String sql = String.format(
                        format,
                        interval,
                        RandomTimeGenerator.R.nextInt(200) - 100);
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for time type (fsp 0) parameter
     */
    @Test
    public void testTimestampAddTime0() {
        // TIMESTAMPADD(unit,interval,datetime_expr)
        String format = "select concat(timestampadd(%s, %s, cast(? as time(0))))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(1)
            )
            .forEach(
                param -> {
                    // timestampadd interval_value cannot be parameterized.
                    // don't know why.
                    String sql = String.format(
                        format,
                        interval,
                        RandomTimeGenerator.R.nextInt(200) - 100);
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for time type (fsp 3) parameter
     */
    @Test
    public void testTimestampAddTime3() {
        // TIMESTAMPADD(unit,interval,datetime_expr)
        String format = "select concat(timestampadd(%s, %s, cast(? as time(3))))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(1)
            )
            .forEach(
                param -> {
                    // timestampadd interval_value cannot be parameterized.
                    // don't know why.
                    String sql = String.format(
                        format,
                        interval,
                        RandomTimeGenerator.R.nextInt(200) - 100);
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }

    /**
     * for time type (fsp 6) parameter
     */
    @Test
    public void testTimestampAddTime6() {
        // TIMESTAMPADD(unit,interval,datetime_expr)
        String format = "select concat(timestampadd(%s, %s, cast(? as time(6))))";

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(1)
            )
            .forEach(
                param -> {
                    // timestampadd interval_value cannot be parameterized.
                    // don't know why.
                    String sql = String.format(
                        format,
                        interval,
                        RandomTimeGenerator.R.nextInt(200) - 100);
                    selectContentSameAssert(sql, param, mysqlConnection, tddlConnection);
                }
            );
    }
}
