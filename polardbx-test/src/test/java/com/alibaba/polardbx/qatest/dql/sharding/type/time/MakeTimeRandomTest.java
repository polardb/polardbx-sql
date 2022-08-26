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

import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class MakeTimeRandomTest extends TimeTestBase {

    @Test
    public void testMakeTime() {
        String sql = "select concat(cast(maketime(?, ?, ?) as time(6)))";

        IntStream.range(0, 1 << 10)
            .mapToObj(
                i -> ImmutableList.of(
                    RandomTimeGenerator.generateHour(1).get(0),
                    RandomTimeGenerator.generateMinute(1).get(0),
                    RandomTimeGenerator.generateSecond(1).get(0)
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
}
