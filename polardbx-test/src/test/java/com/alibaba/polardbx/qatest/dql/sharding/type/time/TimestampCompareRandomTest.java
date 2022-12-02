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
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TimestampCompareRandomTest extends TimeTableTestBase {
    private String fsp;

    static final int COMPARE_DATA_SIZE = 1 << 5;

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

    public TimestampCompareRandomTest(String fsp) {
        super(randomTableName("timestamp_c_fsp_test", 4));
        this.fsp = fsp;
    }

    @Test
    public void testDatetimeDatetime() {
        // datetime op datetime
        List<String> timeStrings1 = RandomTimeGenerator
            .generateNullableDatetimeString(COMPARE_DATA_SIZE)
            .stream()
            .map(String.class::cast)
            .collect(Collectors.toList());
        String col1 = colName(MySQLTimeTypeUtil.DATETIME_SQL_TYPE, Integer.valueOf(fsp));
        List<String> timeStrings2 = RandomTimeGenerator
            .generateNullableDatetimeString(COMPARE_DATA_SIZE)
            .stream()
            .map(String.class::cast)
            .collect(Collectors.toList());
        String col2 = colName(MySQLTimeTypeUtil.DATETIME_SQL_TYPE, Integer.valueOf(fsp));

        testCompareOp(timeStrings1, col1, timeStrings2, col2);
    }

    @Test
    public void testDatetimeTimestamp() {
        // datetime op timestamp
        List<String> timeStrings1 = RandomTimeGenerator
            .generateNullableDatetimeString(COMPARE_DATA_SIZE)
            .stream()
            .map(String.class::cast)
            .collect(Collectors.toList());
        String col1 = colName(MySQLTimeTypeUtil.DATETIME_SQL_TYPE, Integer.valueOf(fsp));
        List<String> timeStrings2 = RandomTimeGenerator
            .generateValidTimestampString(COMPARE_DATA_SIZE)
            .stream()
            .map(String.class::cast)
            .collect(Collectors.toList());
        String col2 = colName(Types.TIMESTAMP, Integer.valueOf(fsp));

        testCompareOp(timeStrings1, col1, timeStrings2, col2);
    }

    @Test
    public void testTimestampDatetime() {
        // timestamp op datetime
        List<String> timeStrings1 = RandomTimeGenerator
            .generateValidTimestampString(COMPARE_DATA_SIZE)
            .stream()
            .map(String.class::cast)
            .collect(Collectors.toList());
        String col1 = colName(Types.TIMESTAMP, Integer.valueOf(fsp));
        List<String> timeStrings2 = RandomTimeGenerator
            .generateNullableDatetimeString(COMPARE_DATA_SIZE)
            .stream()
            .map(String.class::cast)
            .collect(Collectors.toList());
        String col2 = colName(MySQLTimeTypeUtil.DATETIME_SQL_TYPE, Integer.valueOf(fsp));

        testCompareOp(timeStrings1, col1, timeStrings2, col2);
    }

    @Test
    public void testTimestampTimestamp() {
        // timestamp op timestamp
        List<String> timeStrings1 = RandomTimeGenerator
            .generateValidTimestampString(COMPARE_DATA_SIZE)
            .stream()
            .map(String.class::cast)
            .collect(Collectors.toList());
        String col1 = colName(Types.TIMESTAMP, Integer.valueOf(fsp));
        List<String> timeStrings2 = RandomTimeGenerator
            .generateValidTimestampString(COMPARE_DATA_SIZE)
            .stream()
            .map(String.class::cast)
            .collect(Collectors.toList());
        String col2 = colName(Types.TIMESTAMP, Integer.valueOf(fsp));

        testCompareOp(timeStrings1, col1, timeStrings2, col2);
    }
}
