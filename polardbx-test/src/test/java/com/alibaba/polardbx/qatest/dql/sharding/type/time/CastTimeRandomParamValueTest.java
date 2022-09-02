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

public class CastTimeRandomParamValueTest extends TimeTestBase {
    ParamValue paramValue;

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        // time类型会引起 time/datetime mix 操作，涉及本地时区当前时间，无法进行对比测试
        return ParamValue.values(true)
            .stream()
            .map(Enum::name)
            .map(v -> new String[] {v})
            .collect(Collectors.toList());
    }

    public CastTimeRandomParamValueTest(String paramValue) {
        this.paramValue = ParamValue.valueOf(paramValue);
    }

    @Test
    public void testCastDatetime() {
        String sql = String.format("select concat(cast(%s as datetime(4)))", paramValue.format);

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
    public void testCastTime() {
        if (isMySQL80()) {
            //8.0 和 5.7 函数在某些corner case情况下不兼容
            return;
        }

        String sql = String.format("select concat(cast(%s as time(4)))", paramValue.format);

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
    public void testCastDate() {
        String sql = String.format("select concat(cast(%s as date))", paramValue.format);

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
