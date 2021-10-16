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

package com.alibaba.polardbx.druid.bvt.sql.utils;

import com.alibaba.polardbx.druid.sql.ast.expr.SQLTimestampExpr;
import junit.framework.TestCase;

public class SQLDateTimeExprCheck_0 extends TestCase {
    public void test_null() throws Exception {
        assertFalse(SQLTimestampExpr.check("0"));
        assertFalse(SQLTimestampExpr.check(null));
        assertFalse(SQLTimestampExpr.check("0002-11-30 00:00:00"));
    }

    public void test_14() throws Exception {
        assertTrue(SQLTimestampExpr.check("1970-1-1 0:0:0"));
        assertTrue(SQLTimestampExpr.check("1000-1-1 0:0:0"));
        assertTrue(SQLTimestampExpr.check("1000-1-1 0:0:0.0"));
        assertFalse(SQLTimestampExpr.check("1970-A-1 0:0:0"));
        assertFalse(SQLTimestampExpr.check("1970-*-1 0:0:0"));
        assertFalse(SQLTimestampExpr.check("1970-1-A 0:0:0"));
        assertFalse(SQLTimestampExpr.check("1970-1-* 0:0:0"));
    }

    public void test_time() throws Exception {
        assertTrue(SQLTimestampExpr.check("1970-1-1 0:1:2"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 0:1:23"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 0:12:3"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 0:12:34"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 01:2:3"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 01:2:34"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 01:23:4"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 01:23:4"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 01:23:45"));
    }

    public void test_time_hour() throws Exception {
        assertFalse(SQLTimestampExpr.check("1970-1-1 25:23:45"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 24:23:45"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 30:23:45"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 a0:23:45"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 *0:23:45"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 1a:23:45"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 1*:23:45"));

        assertFalse(SQLTimestampExpr.check("1970-1-1 30:23:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 a0:23:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 *0:23:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 1a:23:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 1*:23:4"));

        assertTrue(SQLTimestampExpr.check("1970-1-1 3:2:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 a:2:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1a1:2:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 3:2a4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 3-2:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 *:2:4"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 1:2:4"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 1:2:4"));

        assertTrue(SQLTimestampExpr.check("1970-1-1 13:2:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 13-2:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 13:2-4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1-13:2:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 1a32:4"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 1:32:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 1a:2:4"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 1t2:14"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 1*:2:4"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 11:2:4"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 11:2:4"));

        assertTrue(SQLTimestampExpr.check("1970-1-1 11:2:14"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 11:2t14"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 11t2:14"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 a:12:14"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 *:12:14"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:a:14"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:*:14"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:12:a"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:12:*"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:12t1"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12t12:1"));

        assertFalse(SQLTimestampExpr.check("1970-1-1 12t12:11"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:12t11"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:12:71"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:72:11"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 32:12:11"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 25:12:11"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 20:12:61"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 20:12:a1"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 20:12:*1"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 20:12:1a"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 20:12:1*"));
        assertFalse(SQLTimestampExpr.check("1970-1-1t20:12:11"));
    }

    public void test_time_minute() throws Exception {
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:61:45"));
        assertTrue(SQLTimestampExpr.check("1970-1-1 12:60:45"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:70:45"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:a0:45"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:*0:45"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:1a:45"));
        assertFalse(SQLTimestampExpr.check("1970-1-1 12:1a:45"));
        assertFalse(SQLTimestampExpr.check("1970-11-11 12:1*:45"));
    }

    public void test_1() throws Exception {
        assertFalse(SQLTimestampExpr.check("1000-00-00 00:00:00"));
        assertTrue(SQLTimestampExpr.check("1000-01-01 00:00:00"));
        assertTrue(SQLTimestampExpr.check("1970-01-01 00:00:00"));
        assertTrue(SQLTimestampExpr.check("1970-02-28 00:00:00"));
        assertFalse(SQLTimestampExpr.check("1970-02-30 00:00:00"));
        assertTrue(SQLTimestampExpr.check("1970-2-28 00:00:00"));
        assertTrue(SQLTimestampExpr.check("1970-2-2 00:00:00"));
        assertTrue(SQLTimestampExpr.check("1970-12-2 00:00:00"));
        assertTrue(SQLTimestampExpr.check("1970-11-30 00:00:00"));
        assertFalse(SQLTimestampExpr.check("1970-11-31 00:00:00"));
        assertTrue(SQLTimestampExpr.check("1970-12-31 00:00:00"));
        assertFalse(SQLTimestampExpr.check("1970-12-32 00:00:00"));
        assertFalse(SQLTimestampExpr.check("1970-12-00 00:00:00"));
        assertFalse(SQLTimestampExpr.check("1970-12-A0 00:00:00"));
        assertFalse(SQLTimestampExpr.check("1970-22-A0 00:00:00"));
        assertTrue(SQLTimestampExpr.check("1970-13-01 00:00:00"));
        assertFalse(SQLTimestampExpr.check("1970-00-01 00:00:00"));

        assertTrue(SQLTimestampExpr.check("9999-12-2 00:00:00"));
        assertFalse(SQLTimestampExpr.check("A999-12-2 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9A99-12-2 00:00:00"));
        assertFalse(SQLTimestampExpr.check("99A9-12-2 00:00:00"));
        assertFalse(SQLTimestampExpr.check("999A-12-2 00:00:00"));

        assertFalse(SQLTimestampExpr.check("*999-12-2 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9*99-12-2 00:00:00"));
        assertFalse(SQLTimestampExpr.check("99*9-12-2 00:00:00"));
        assertFalse(SQLTimestampExpr.check("999*-12-2 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9999*12-2 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9999-12*2 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9999-1*2 00:00:00"));

        assertFalse(SQLTimestampExpr.check("9999-11-*2 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9999-11-A2 00:00:00"));

        assertFalse(SQLTimestampExpr.check("9999-11-1A 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9999-11-1* 00:00:00"));

        assertFalse(SQLTimestampExpr.check("9999-11-A 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9999-11-* 00:00:00"));

        assertFalse(SQLTimestampExpr.check("9999-1-A 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9999-1-* 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9999-1-1A 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9999-1-1* 00:00:00"));

        assertTrue(SQLTimestampExpr.check("9999-1-11 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9999-1-111 00:00:00"));

        assertFalse(SQLTimestampExpr.check("9999-1a-11 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9999-1*-11 00:00:00"));

        assertFalse(SQLTimestampExpr.check("9999-*-1 00:00:00"));
        assertFalse(SQLTimestampExpr.check("9999-A-1 00:00:00"));

        assertTrue(SQLTimestampExpr.check("2001-01-01 00:00:00"));
        assertTrue(SQLTimestampExpr.check("2001-13-01 00:00:00"));
        assertFalse(SQLTimestampExpr.check("2001-00-011 00:00:00"));
    }
}
