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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExprGroup;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;

public class BinaryOpSplitTest extends TestCase {
    public void test_for_split_0() throws Exception {
        String sql = "select * from t where f1 = 0 or f1 = 1 or f1 = 2";
        SQLSelectStatement stmt = (SQLSelectStatement)
                SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.EnableSQLBinaryOpExprGroup)
                        .get(0);

        MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) stmt.getSelect().getQuery();
        SQLBinaryOpExprGroup group = (SQLBinaryOpExprGroup) queryBlock.getWhere();
        assertEquals(3, group.getItems().size());
        assertEquals("f1 = 0", group.getItems().get(0).toString());
        assertEquals("f1 = 1", group.getItems().get(1).toString());
        assertEquals("f1 = 2", group.getItems().get(2).toString());
    }

    public void test_for_split_1() throws Exception {
        String sql = "select * from t where f1 = 0 or f1 = 1 or f1 = 2";
        SQLSelectStatement stmt = (SQLSelectStatement)
                SQLUtils.parseStatements(sql, JdbcConstants.MYSQL)
                        .get(0);

        MySqlSelectQueryBlock queryBlock = (MySqlSelectQueryBlock) stmt.getSelect().getQuery();
        SQLBinaryOpExpr where = (SQLBinaryOpExpr) queryBlock.getWhere();
        List<SQLExpr> items = SQLBinaryOpExpr.split(where);
        assertEquals(3, items.size());
        assertEquals("f1 = 0", items.get(0).toString());
        assertEquals("f1 = 1", items.get(1).toString());
        assertEquals("f1 = 2", items.get(2).toString());
    }
}
