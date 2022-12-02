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

public class PeriodAddDiffRandomTest extends TimeTestBase {
    @Test
    public void testPeriodAdd() {
        if (isMySQL80()) {
            //8.0 和 5.7 函数在某些corner case情况下不兼容
            return;
        }

        String sql = "select period_add(?, ?)";

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> ImmutableList.of(
                    RandomTimeGenerator.generatePeriod(1).get(0),
                    RandomTimeGenerator.R.nextInt(999)
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
    public void testPeriodDiff() {
        if (isMySQL80()) {
            return;
        }
        String sql = "select period_diff(?, ?)";

        IntStream.range(0, DEFAULT_CHUNK_SIZE)
            .mapToObj(
                i -> RandomTimeGenerator.generatePeriod(2)
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
