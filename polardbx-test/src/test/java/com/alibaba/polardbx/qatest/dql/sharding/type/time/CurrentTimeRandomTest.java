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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;


public class CurrentTimeRandomTest extends TimeTestBase {
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

    public CurrentTimeRandomTest(String fsp) {
        this.fsp = fsp;
    }

    @Test
    public void testNow() {
        String sql = String.format("select length(concat(now(%s)))", fsp);
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testNowNoParam() {
        String sql = String.format("select length(concat(now()))", fsp);
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testCurDate() {
        String sql = String.format("select concat(curdate())");
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testCurrentDate() {
        String sql = String.format("select concat(current_date)");
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testCurTimeNoParam() {
        String sql = String.format("select length(concat(current_time))");
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testCurTime() {
        String sql = String.format("select length(concat(curtime(%s)))", fsp);
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testCurrentTime() {
        String sql = String.format("select length(concat(current_time(%s)))", fsp);
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testCurrentTimestamp() {
        String sql = String.format("select length(concat(current_timestamp(%s)))", fsp);
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testCurrentTimestampNoParam() {
        String sql = String.format("select length(concat(current_timestamp))");
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testLocalTimeNoParam() {
        String sql = String.format("select length(concat(localtime))");
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testLocalTime() {
        String sql = String.format("select length(concat(localtime(%s)))", fsp);
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testLocalTimestampNoParam() {
        String sql = String.format("select length(concat(localtimestamp))");
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testLocalTimestamp() {
        String sql = String.format("select length(concat(localtimestamp(%s)))", fsp);
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testSysdate() {
        String sql = String.format("select length(concat(sysdate(%s)))", fsp);
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testSysdateNoParam() {
        String sql = String.format("select length(concat(sysdate()))", fsp);
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testUTCDate() {
        String sql = String.format("select length(concat(utc_date()))", fsp);
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testUTCTime() {
        String sql = String.format("select length(concat(utc_time(%s)))", fsp);
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testUTCTimeNoParam() {
        String sql = String.format("select length(concat(utc_time()))");
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testUTCTimestampNoParam() {
        String sql = String.format("select length(concat(utc_timestamp()))");
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }

    @Test
    public void testUTCTimestamp() {
        String sql = String.format("select length(concat(utc_timestamp(%s)))", fsp);
        List<Object> params = new ArrayList<>();

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .forEach(
                i -> selectContentSameAssert(sql, params, mysqlConnection, tddlConnection)
            );
    }
}
