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
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import org.junit.Assume;
import org.junit.Before;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.isMySQL80;

public abstract class TimeTestBase extends ReadBaseTestCase {
    protected static final boolean ALLOW_TIME_TYPE = false;
    protected static final int DEFAULT_CHUNK_SIZE = 1 << 6;
    protected static final int DATA_SIZE = 1 << 10;

    protected String colName(int sqlType, int scale) {
        switch (sqlType) {
        case Types.TIMESTAMP:
            return "timestamp_" + scale;
        case MySQLTimeTypeUtil.DATETIME_SQL_TYPE:
            return "datetime_" + scale;
        case Types.DATE:
            return "date_0";
        case Types.TIME:
            return "time_" + scale;
        default:
            return null;
        }
    }

    @Before
    public void assureMySQL57() {
        Assume.assumeFalse("ignore this test case in mysql 8.0 version.", isMySQL80());
    }

    /**
     * param value is used for getting parameter value.
     * 避免测试代码 code explosion
     * sqlType = 通用sql type 编号
     * format = sql格式
     * getter = 取值方式
     */
    protected enum ParamValue {
        // invalid value, like null / random string / random double value.
        INVALID(Types.OTHER, "?", i -> RandomTimeGenerator.generateInvalidParam(i)),

        // numeric value
        NUMERIC_DATETIME(Types.NUMERIC, "?", i -> RandomTimeGenerator.generateValidDatetimeNumeric(i)),
        NUMERIC_TIME(Types.NUMERIC, "?", i -> RandomTimeGenerator.generateValidTimeNumeric(i)),

        // string value
        STRING_DATETIME(Types.VARCHAR, "?", i -> RandomTimeGenerator.generateValidDatetimeString(i)),
        STRING_TIME(Types.VARCHAR, "?", i -> RandomTimeGenerator.generateValidTimeString(i)),

        // time type value
        TIME_0(Types.TIME, "cast(? as time(0))", i -> RandomTimeGenerator.generateValidTimeString(i)),
        TIME_1(Types.TIME, "cast(? as time(1))", i -> RandomTimeGenerator.generateValidTimeString(i)),
        TIME_2(Types.TIME, "cast(? as time(2))", i -> RandomTimeGenerator.generateValidTimeString(i)),
        TIME_3(Types.TIME, "cast(? as time(3))", i -> RandomTimeGenerator.generateValidTimeString(i)),
        TIME_4(Types.TIME, "cast(? as time(4))", i -> RandomTimeGenerator.generateValidTimeString(i)),
        TIME_5(Types.TIME, "cast(? as time(5))", i -> RandomTimeGenerator.generateValidTimeString(i)),
        TIME_6(Types.TIME, "cast(? as time(6))", i -> RandomTimeGenerator.generateValidTimeString(i)),

        // datetime / timestamp type value
        DATETIME_0(Types.TIMESTAMP, "cast(? as datetime(0))", i -> RandomTimeGenerator.generateValidDatetimeString(i)),
        DATETIME_1(Types.TIMESTAMP, "cast(? as datetime(1))", i -> RandomTimeGenerator.generateValidDatetimeString(i)),
        DATETIME_2(Types.TIMESTAMP, "cast(? as datetime(2))", i -> RandomTimeGenerator.generateValidDatetimeString(i)),
        DATETIME_3(Types.TIMESTAMP, "cast(? as datetime(3))", i -> RandomTimeGenerator.generateValidDatetimeString(i)),
        DATETIME_4(Types.TIMESTAMP, "cast(? as datetime(4))", i -> RandomTimeGenerator.generateValidDatetimeString(i)),
        DATETIME_5(Types.TIMESTAMP, "cast(? as datetime(5))", i -> RandomTimeGenerator.generateValidDatetimeString(i)),
        DATETIME_6(Types.TIMESTAMP, "cast(? as datetime(6))", i -> RandomTimeGenerator.generateValidDatetimeString(i)),

        // date value.
        DATE(Types.DATE, "cast(? as date)", i -> RandomTimeGenerator.generateValidDatetimeString(i));

        int sqlType;
        String format;
        Function<Integer, List<Object>> getter;

        ParamValue(int sqlType, String format, Function<Integer, List<Object>> getter) {
            this.sqlType = sqlType;
            this.format = format;
            this.getter = getter;
        }

        public static List<ParamValue> values(boolean useTimeType) {
            return Arrays.stream(values())
                .filter(v -> useTimeType || v.sqlType != Types.TIME)
                .collect(Collectors.toList());
        }
    }
}
