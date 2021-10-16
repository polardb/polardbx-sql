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

package com.alibaba.polardbx.transaction.rawsql;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RawSqlUtilsTest {

    private TimeZone defaultTimeZone;

    @Before
    public void before() {
        defaultTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    @After
    public void after() {
        TimeZone.setDefault(defaultTimeZone);
    }

    @Test
    public void testFormatParameter_Null() {
        String nullValue = RawSqlUtils.formatParameter(null);
        assertEquals("NULL", nullValue);
    }

    @Test
    public void testFormatParameter_Boolean() {
        String booleanVaue = RawSqlUtils.formatParameter(true);
        assertEquals("1", booleanVaue);
    }
    
    @Test
    public void testFormatParameter_Number() {
        String numberValue = RawSqlUtils.formatParameter(123);
        assertEquals("123", numberValue);
    }

    @Test
    public void testFormatParameter_BigDecimal() {
        String numberValue = RawSqlUtils.formatParameter(new BigDecimal("1234567.89"));
        assertEquals("1234567.89", numberValue);
    }

    @Test
    public void testFormatParameter_String() {
        String stringValue = RawSqlUtils.formatParameter("tddl5");
        assertEquals("'tddl5'", stringValue);
    }

    @Test
    public void testFormatParameter_Date() {
        String dateValue = RawSqlUtils.formatParameter(new Date(1512366731000L));
        assertEquals("'2017-12-04'", dateValue);
    }

    @Test
    public void testFormatParameter_Time() {
        String dateValue = RawSqlUtils.formatParameter(new Time(1512366731000L));
        assertEquals("'05:52:11'", dateValue);
    }

    @Test
    public void testFormatParameter_Timestamp() {
        String dateValue = RawSqlUtils.formatParameter(new Timestamp(1512366731000L));
        assertEquals("'2017-12-04 05:52:11.000'", dateValue);
    }

    @Test
    public void testFormatParameter_ByteArray() {
        String dateValue = RawSqlUtils.formatParameter("hello".getBytes());
        assertEquals("X'68656C6C6F'", dateValue);
    }
}
