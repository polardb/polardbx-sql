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

package com.alibaba.polardbx.druid.bvt.sql.refactor;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitorAdapter;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

public class ClearSchema_0 extends TestCase {
    public void test_insert_0() throws Exception {
        String sql = "INSERT INTO testdb.Websites (name, country)\n" +
                "SELECT app_name, country FROM testdb.apps;";

        SQLStatement stmt = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL).get(0);

        SQLASTVisitor v = new SQLASTVisitorAdapter() {
            @Override
            public boolean visit(SQLPropertyExpr x) {
                if (SQLUtils.replaceInParent(x, new SQLIdentifierExpr(x.getName()))) {
                    return false;
                }
                return super.visit(x);
            }
        };
        stmt.accept(v);

        assertEquals("INSERT INTO Websites (name, country)\n" +
                "SELECT app_name, country\n" +
                "FROM apps;", stmt.toString());
    }
}
