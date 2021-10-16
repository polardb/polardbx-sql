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

package com.alibaba.polardbx.druid.bvt.sql.mysql.api;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import junit.framework.TestCase;

public class APITest extends TestCase {
    public void test_0() throws Exception {
        String sql = "select * from t";
        SQLSelectStatement stmt = (SQLSelectStatement) SQLUtils.parseStatements(sql, DbType.mysql).get(0);
        SQLExprTableSource tableSource = (SQLExprTableSource) stmt.getSelect().getQueryBlock().getFrom();
        assertNull(tableSource.getSchema());
        assertNull(tableSource.getCatalog());
    }

    public void test_1() throws Exception {
        String sql = "select * from d1.t";
        SQLSelectStatement stmt = (SQLSelectStatement) SQLUtils.parseStatements(sql, DbType.mysql).get(0);
        SQLExprTableSource tableSource = (SQLExprTableSource) stmt.getSelect().getQueryBlock().getFrom();
        assertEquals("d1", tableSource.getSchema());
        assertNull(tableSource.getCatalog());
    }

    public void test_2() throws Exception {
        String sql = "select * from c1.d1.t";
        SQLSelectStatement stmt = (SQLSelectStatement) SQLUtils.parseStatements(sql, DbType.mysql).get(0);
        SQLExprTableSource tableSource = (SQLExprTableSource) stmt.getSelect().getQueryBlock().getFrom();
        assertEquals("d1", tableSource.getSchema());
        assertEquals("c1", tableSource.getCatalog());
    }
}
