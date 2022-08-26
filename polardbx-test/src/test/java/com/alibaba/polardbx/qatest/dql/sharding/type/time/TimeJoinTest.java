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

import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.RandomTimeGenerator;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TimeJoinTest extends TimeTableTestBase {
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

    public TimeJoinTest(String fsp) {
        super(randomTableName("time_fsp_test", 4));
        this.fsp = fsp;
    }

    @Test
    public void testDatetime() {
        // datetime string
        List<String> timeStrings = RandomTimeGenerator
            .generateValidDatetimeString(DATA_SIZE)
            .stream()
            .map(String.class::cast)
            .collect(Collectors.toList());
        // col name
        String col = colName(MySQLTimeTypeUtil.DATETIME_SQL_TYPE, Integer.valueOf(fsp));

        testJoin(timeStrings, col);
    }

    @Ignore
    @Test
    public void testTimestamp() {
        // datetime string
        List<String> timeStrings = RandomTimeGenerator
            .generateValidTimestampString(DATA_SIZE)
            .stream()
            .map(String.class::cast)
            .collect(Collectors.toList());
        // col name
        String col = colName(Types.TIMESTAMP, Integer.valueOf(fsp));

        testJoin(timeStrings, col);
    }

    @Test
    public void testTime() {
        // datetime string
        List<String> timeStrings = RandomTimeGenerator
            .generateValidTimeString(DATA_SIZE)
            .stream()
            .map(String.class::cast)
            .collect(Collectors.toList());
        // col name
        String col = colName(Types.TIME, Integer.valueOf(fsp));

        testJoin(timeStrings, col);
    }

    @Test
    public void testDate() {
        // datetime string
        List<String> timeStrings = RandomTimeGenerator
            .generateValidDatetimeString(DATA_SIZE)
            .stream()
            .map(String.class::cast)
            .collect(Collectors.toList());
        // col name
        String col = colName(Types.DATE, Integer.valueOf(fsp));

        testJoin(timeStrings, col);
    }
}
