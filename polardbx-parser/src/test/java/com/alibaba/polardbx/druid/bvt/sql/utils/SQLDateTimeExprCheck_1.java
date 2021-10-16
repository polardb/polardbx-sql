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

import java.util.Date;
import java.util.TimeZone;

public class SQLDateTimeExprCheck_1 extends TestCase {

    public void test_14() throws Exception {
        assertTrue(SQLTimestampExpr.check("2018-10-11 01:02:03.333"));

        Date date = new SQLTimestampExpr("2018-10-11 01:02:03.333").getDate(TimeZone.getTimeZone("GMT+8:00"));
    }


}
