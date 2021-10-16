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

import com.alibaba.polardbx.druid.sql.ast.expr.SQLTimeExpr;
import junit.framework.TestCase;

public class SQLTimeExprCheck_0 extends TestCase {
    public void test_0() throws Exception {
        assertFalse(SQLTimeExpr.check(null));
        assertFalse(SQLTimeExpr.check(""));
        assertFalse(SQLTimeExpr.check("88"));
        assertFalse(SQLTimeExpr.check("25:25:25"));
        assertFalse(SQLTimeExpr.check("01:61:25"));
        assertFalse(SQLTimeExpr.check("01:01:61"));
        assertTrue(SQLTimeExpr.check("01:01:01"));
        assertTrue(SQLTimeExpr.check("00:00:00"));
        assertTrue(SQLTimeExpr.check("24:00:00"));
    }
}
