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

/**
 * TIMESTAMPDIFF(unit,datetime_expr1,datetime_expr2)
 * Returns datetime_expr2 − datetime_expr1, where datetime_expr1 and datetime_expr2
 * are date or datetime expressions. One expression may be a date and the other a datetime; a date
 * value is treated as a datetime having the time part '00:00:00' where necessary. The unit for the
 * result (an integer) is given by the unit argument. The legal values for unit are the same as those
 * listed in the description of the TIMESTAMPADD() function.
 */

public class TimestampDiffRandomTest extends TimeTestBase {
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

    public TimestampDiffRandomTest(String interval) {
        this.interval = interval;
    }

    /**
     * for datetime string parameter
     */
    @Test
    public void testTimestampDiff() {
        String format = "select concat(timestampdiff(%s, ?, ?))";
        String sql = String.format(format, interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(2)
            )
            .forEach(
                param -> {
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }

    /**
     * for date type parameter
     */
    @Test
    public void testTimestampDiffDate() {
        String format = "select concat(timestampdiff(%s, cast(? as date), cast(? as date)))";
        String sql = String.format(format, interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(2)
            )
            .forEach(
                param -> {
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }

    /**
     * for datetime type (fsp 0) parameter
     */
    @Test
    public void testTimestampDiffDatetime0() {
        String format = "select concat(timestampdiff(%s, cast(? as datetime(0)), cast(? as datetime(0))))";
        String sql = String.format(format, interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(2)
            )
            .forEach(
                param -> {
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }

    /**
     * for datetime type (fsp 3) parameter
     */
    @Test
    public void testTimestampDiffDatetime3() {
        String format = "select concat(timestampdiff(%s, cast(? as datetime(3)), cast(? as datetime(3))))";
        String sql = String.format(format, interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(2)
            )
            .forEach(
                param -> {
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }

    /**
     * for datetime type (fsp 0) parameter
     */
    @Test
    public void testTimestampDiffDatetime6() {
        String format = "select concat(timestampdiff(%s, cast(? as datetime(6)), cast(? as datetime(6))))";
        String sql = String.format(format, interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(2)
            )
            .forEach(
                param -> {
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }

    /**
     * for time type (fsp 0) parameter
     */
    @Test
    public void testTimestampDiffTime0() {
        String format = "select concat(timestampdiff(%s, cast(? as time(0)), cast(? as time(0))))";
        String sql = String.format(format, interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(2)
            )
            .forEach(
                param -> {
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }

    /**
     * for time type (fsp 3) parameter
     */
    @Test
    public void testTimestampDiffTime3() {
        String format = "select concat(timestampdiff(%s, cast(? as time(3)), cast(? as time(3))))";
        String sql = String.format(format, interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(2)
            )
            .forEach(
                param -> {
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }

    /**
     * for time type (fsp 6) parameter
     */
    @Test
    public void testTimestampDiffTime6() {
        String format = "select concat(timestampdiff(%s, cast(? as time(6)), cast(? as time(6))))";
        String sql = String.format(format, interval);

        // run select date_add (col, interval ? unit)
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeString(2)
            )
            .forEach(
                param -> {
                    selectContentSameAssert(
                        sql,
                        param,
                        mysqlConnection,
                        tddlConnection);
                }
            );
    }
}
