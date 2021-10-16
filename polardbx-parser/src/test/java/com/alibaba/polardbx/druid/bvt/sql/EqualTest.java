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

package com.alibaba.polardbx.druid.bvt.sql;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import junit.framework.TestCase;

public class EqualTest extends TestCase {
    public void test_eq_0() throws Exception {
        SQLExpr e0 = SQLUtils.toSQLExpr("T0.name = 'abc'", DbType.mysql);
        SQLExpr e1 = SQLUtils.toSQLExpr("T0.NAME = 'abc'", DbType.mysql);
        assertEquals(e0, e1);
    }
}
