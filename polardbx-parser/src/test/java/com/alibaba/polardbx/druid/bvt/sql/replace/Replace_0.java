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

package com.alibaba.polardbx.druid.bvt.sql.replace;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

public class Replace_0 extends TestCase {
    public void test_0() throws Exception {
        String sql = "select f1 from t where fid = 3";

        SQLSelectStatement stmt = (SQLSelectStatement) SQLUtils.parseStatements(sql, JdbcConstants.MYSQL).get(0);

        assertEquals("SELECT f1\n" +
                "FROM t\n" +
                "WHERE fid = 3", stmt.toString());

        SQLSelectQueryBlock queryBlock = stmt.getSelect().getQueryBlock();

        SQLExpr expr = queryBlock.getSelectItem(0).getExpr();
        assertEquals("f1", expr.toString());

        assertTrue(SQLUtils.replaceInParent(expr, new SQLIdentifierExpr("f2")));

        assertEquals("SELECT f2\n" +
                "FROM t\n" +
                "WHERE fid = 3", stmt.toString());

        SQLExpr where = queryBlock.getWhere();
        assertEquals("fid = 3", where.toString());
        assertTrue(
                SQLUtils.replaceInParent(where
                    , new SQLBinaryOpExpr(
                            new SQLIdentifierExpr("fid")
                                , SQLBinaryOperator.Equality
                                , new SQLIntegerExpr(4))));

        assertEquals("SELECT f2\n" +
                "FROM t\n" +
                "WHERE fid = 4", stmt.toString());
    }
}
