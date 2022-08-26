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

import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isMySQL80;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class TimeConstructionFunctionRandomTest extends TimeTestBase {
    TimeTestBase.ParamValue paramValue;

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return ParamValue.values(true)
            .stream()
            .map(Enum::name)
            .map(v -> new String[] {v})
            .collect(Collectors.toList());
    }

    public TimeConstructionFunctionRandomTest(String paramValue) {
        this.paramValue = TimeTestBase.ParamValue.valueOf(paramValue);
    }

    /**
     * FROM_DAYS(N)
     * Given a day number N, returns a DATE value.
     */
    @Test
    public void testFromDays() {
        String sql = String.format("select concat(from_days(%s))", paramValue.format);

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
     * FROM_DAYS(N)
     * Given a day number N, returns a DATE value.
     */
    @Test
    public void testSecToTime() {
        String sql = String.format("select concat(cast(sec_to_time(%s) as time(6)))", paramValue.format);

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
    public void testTimestamp() {
        String sql = String.format("select concat(cast(timestamp(%s) as datetime(6)))", paramValue.format);

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
    public void testTime() {
        if (isMySQL80()) {
            //8.0 和 5.7 函数在某些corner case情况下不兼容
            return;
        }

        String sql = String.format("select concat(cast(time(%s) as time(6)))", paramValue.format);

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
    public void testDate() {
        String sql = String.format("select concat(date(%s))", paramValue.format);

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
    public void testTimeToSec() {
        if (isMySQL80()) {
            //8.0 和 5.7 函数在某些corner case情况下不兼容
            return;
        }

        String sql = String.format("select time_to_sec(%s)", paramValue.format);

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
    public void testToSeconds() {
        String sql = String.format("select to_seconds(%s)", paramValue.format);

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
