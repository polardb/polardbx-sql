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

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;


public class UnixTimestampRandomTest extends TimeTestBase {
    TimeTestBase.ParamValue paramValue;

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return ParamValue.values(true)
            .stream()
            .map(Enum::name)
            .map(v -> new String[] {v})
            .collect(Collectors.toList());
    }

    public UnixTimestampRandomTest(String paramValue) {
        this.paramValue = TimeTestBase.ParamValue.valueOf(paramValue);
    }

    @Before
    public void setTimeZone() {
        JdbcUtil.executeQuery("set time_zone = '+08:00'", mysqlConnection);
        JdbcUtil.executeQuery("set time_zone = '+08:00'", tddlConnection);
    }

    @Test
    public void testUnixTimestamp() {
        String sql = String.format("select cast(unix_timestamp(%s) as decimal(65, 6))", paramValue.format);

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
