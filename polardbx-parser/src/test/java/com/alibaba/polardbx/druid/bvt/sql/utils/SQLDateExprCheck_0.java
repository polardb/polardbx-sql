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

import com.alibaba.polardbx.druid.sql.ast.expr.SQLDateExpr;
import junit.framework.TestCase;

public class SQLDateExprCheck_0 extends TestCase {
    public void test_0() throws Exception {
        assertFalse(SQLDateExpr.check("0"));
        assertFalse(SQLDateExpr.check(null));
        assertFalse(SQLDateExpr.check("1000-00-00"));
        assertFalse(SQLDateExpr.check("0000-05-16"));
        assertTrue(SQLDateExpr.check("1000-01-01"));
        assertTrue(SQLDateExpr.check("1970-01-01"));
        assertTrue(SQLDateExpr.check("1970-02-28"));
        assertFalse(SQLDateExpr.check("1970-02-30"));
        assertTrue(SQLDateExpr.check("1970-2-28"));
        assertTrue(SQLDateExpr.check("1970-2-2"));
        assertTrue(SQLDateExpr.check("1970-12-2"));
        assertTrue(SQLDateExpr.check("1970-11-30"));
        assertFalse(SQLDateExpr.check("1970-11-31"));
        assertTrue(SQLDateExpr.check("1970-12-31"));
        assertFalse(SQLDateExpr.check("1970-12-32"));
        assertFalse(SQLDateExpr.check("1970-12-00"));
        assertFalse(SQLDateExpr.check("1970-12-A0"));
        assertFalse(SQLDateExpr.check("1970-22-A0"));
        assertFalse(SQLDateExpr.check("1970-13-01"));
        assertFalse(SQLDateExpr.check("1970-00-01"));

        assertTrue(SQLDateExpr.check("9999-12-2"));
        assertFalse(SQLDateExpr.check("A999-12-2"));
        assertFalse(SQLDateExpr.check("9A99-12-2"));
        assertFalse(SQLDateExpr.check("99A9-12-2"));
        assertFalse(SQLDateExpr.check("999A-12-2"));

        assertFalse(SQLDateExpr.check("*999-12-2"));
        assertFalse(SQLDateExpr.check("9*99-12-2"));
        assertFalse(SQLDateExpr.check("99*9-12-2"));
        assertFalse(SQLDateExpr.check("999*-12-2"));
        assertFalse(SQLDateExpr.check("9999*12-2"));
        assertFalse(SQLDateExpr.check("9999-12*2"));
        assertFalse(SQLDateExpr.check("9999-1*2"));

        assertFalse(SQLDateExpr.check("9999-11-*2"));
        assertFalse(SQLDateExpr.check("9999-11-A2"));

        assertFalse(SQLDateExpr.check("9999-11-1A"));
        assertFalse(SQLDateExpr.check("9999-11-1*"));

        assertFalse(SQLDateExpr.check("9999-11-A"));
        assertFalse(SQLDateExpr.check("9999-11-*"));

        assertFalse(SQLDateExpr.check("9999-1-A"));
        assertFalse(SQLDateExpr.check("9999-1-*"));
        assertFalse(SQLDateExpr.check("9999-1-1A"));
        assertFalse(SQLDateExpr.check("9999-1-1*"));

        assertTrue(SQLDateExpr.check("9999-1-11"));
        assertFalse(SQLDateExpr.check("9999-1-111"));

        assertFalse(SQLDateExpr.check("9999-1a-11"));
        assertFalse(SQLDateExpr.check("9999-1*-11"));

        assertFalse(SQLDateExpr.check("9999-*-1"));
        assertFalse(SQLDateExpr.check("9999-A-1"));

        assertTrue(SQLDateExpr.check("2001-01-01"));
        assertFalse(SQLDateExpr.check("2001-13-01"));
        assertFalse(SQLDateExpr.check("2001-00-011"));
        assertFalse(SQLDateExpr.check("2001-01-011 00:00:00"));
    }
}
