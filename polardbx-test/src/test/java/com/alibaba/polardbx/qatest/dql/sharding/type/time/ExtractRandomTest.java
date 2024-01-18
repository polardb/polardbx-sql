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


public class ExtractRandomTest extends TimeTestBase {
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

    public ExtractRandomTest(String interval) {
        this.interval = interval;
    }

    /**
     * for datetime string parameter
     */
    @Test
    public void testExtractString() {
        String sql = String.format("select extract(%s FROM ?)", interval);
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateDatetimeString(1)
            )
            .forEach(
                param -> selectContentSameAssert(sql, param, mysqlConnection, tddlConnection)
            );
    }

    /**
     * for datetime string parameter
     */
    @Test
    public void testExtractNumeric() {
        String sql = String.format("select extract(%s FROM ?)", interval);
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateValidDatetimeNumeric(1)
            )
            .forEach(
                param -> selectContentSameAssert(sql, param, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testExtractDatetime() {
        String sql = String.format("select extract(%s FROM cast(? as datetime(3)))", interval);
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateDatetimeString(1)
            )
            .forEach(
                param -> selectContentSameAssert(sql, param, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testExtractDate() {
        String sql = String.format("select extract(%s FROM cast(? as date))", interval);
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateDatetimeString(1)
            )
            .forEach(
                param -> selectContentSameAssert(sql, param, mysqlConnection, tddlConnection)
            );
    }

    /**
     * 当date参数为time类型，并且interval = DAY ~ YEAR，需要
     * 将time和当前时区时间混合形成datetime。
     */
    @Test
    public void testExtractTime() {
        String sql = String.format("select extract(%s FROM cast(? as time(3)))", interval);
        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generateTimeString(1)
            )
            .forEach(
                param -> selectContentSameAssert(sql, param, mysqlConnection, tddlConnection)
            );
    }
}
