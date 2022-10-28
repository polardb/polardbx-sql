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

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * EXTRACT()EXTRACT(unit FROM date) void Item_extract::fix_length_and_dec()
 * DAY()    longlong Item_func_dayofmonth::val_int()
 * DAYNAME()    void Item_func_dayname::fix_length_and_dec()
 * DAYOFMONTH() longlong Item_func_dayofmonth::val_int()
 * DAYOFWEEK()  longlong Item_func_weekday::val_int() odbcType = true
 * DAYOFYEAR()  longlong Item_func_dayofyear::val_int()
 * MICROSECOND()    longlong Item_func_microsecond::val_int()
 * HOUR() HOUR(time)    longlong Item_func_hour::val_int()
 * MINUTE()longlong  Item_func_minute::val_int()
 * MONTH()longlong   Item_func_month::val_int()
 * MONTHNAME()  void Item_func_monthname::fix_length_and_dec()
 * WEEK()WEEK(date[,mode])  longlong Item_func_week::val_int() weekmode default = 0
 * WEEKDAY()    longlong Item_func_weekday::val_int()odbcType = false
 * WEEKOFYEAR() Create_func_weekofyear::create(THD *thd, Item *arg1)    arg1(weekmode) = 3  longlong Item_func_week::val_int()
 * YEAR()   longlong Item_func_year::val_int()
 * YEARWEEK()   longlong Item_func_yearweek::val_int()
 * QUARTER()QUARTER(date)   longlong Item_func_quarter::val_int()
 * SECOND()SECOND(time) longlong Item_func_second::val_int()
 */

public class TimeValueExtractionFunctionRandomTest extends TimeTestBase {
    ParamValue paramValue;

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.stream(ParamValue.values())
            .map(Enum::name)
            .map(v -> new String[] {v})
            .collect(Collectors.toList());
    }

    public TimeValueExtractionFunctionRandomTest(String paramValue) {
        this.paramValue = ParamValue.valueOf(paramValue);
    }

    /**
     * TO_DAYS(date)
     * Given a date date, returns a day number (the number of days since year 0).
     * test date = string
     */
    @Test
    public void testToDays() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select to_days(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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
     * DATEDIFF(expr1,expr2)
     * DATEDIFF() returns expr1 − expr2 expressed as a value in days from one date to the other.
     * expr1 and expr2 are date or date-and-time expressions. Only the date parts of the values are used
     * in the calculation.
     */
    @Test
    public void testDateDiff() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select datediff(%s, %s)", paramValue.format, paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(2)
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

    @Test
    public void testMicrosecond() {
        if (isMySQL80()) {
            //8.0 和 5.7 函数在某些corner case情况下不兼容
            return;
        }

        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select microsecond(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testSecond() {
        if (isMySQL80()) {
            //8.0 和 5.7 函数在某些corner case情况下不兼容
            return;
        }

        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select second(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testMinute() {

        if (isMySQL80()) {
            return;
        }
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select minute(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testHour() {
        if (isMySQL80()) {
            //8.0 和 5.7 函数在某些corner case情况下不兼容
            return;
        }

        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select hour(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testDay() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select day(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testDayOfMonth() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select dayofmonth(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testDayName() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select dayname(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testDayOfWeek() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select dayofweek(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testDayOfYear() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select dayofyear(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testMonth() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select month(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testMonthName() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select monthname(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testYear() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select year(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testWeekDay() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select weekday(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testWeek() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select week(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testWeekWithMode() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME || paramValue.sqlType == Types.OTHER) {
            return;
        }
        String sql = String.format("select week(%s, ?)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> ImmutableList.of(
                    paramValue.getter.apply(1).get(0),
                    RandomTimeGenerator.R.nextInt(8)
                )
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

    @Test
    public void testWeekOfYear() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select weekofyear(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testYearWeek() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select yearweek(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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

    @Test
    public void testQuarter() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        if (paramValue.sqlType == Types.TIME) {
            return;
        }
        String sql = String.format("select quarter(%s)", paramValue.format);

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> paramValue.getter.apply(1)
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
