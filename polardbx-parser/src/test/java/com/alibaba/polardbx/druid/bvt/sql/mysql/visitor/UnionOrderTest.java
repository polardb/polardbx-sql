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

package com.alibaba.polardbx.druid.bvt.sql.mysql.visitor;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQueryTableSource;
import junit.framework.TestCase;

public class UnionOrderTest extends TestCase {
    public void test_0() throws Exception {
        String sql = "select * from (select f1 from t1 union all select f1 from t2 union all select f1 from t3)";

        SQLSelectStatement stmt = (SQLSelectStatement) SQLUtils.parseSingleMysqlStatement(sql);

        assertEquals("SELECT *\n" +
                "FROM (\n" +
                "\tSELECT f1\n" +
                "\tFROM t1\n" +
                "\tUNION ALL\n" +
                "\tSELECT f1\n" +
                "\tFROM t2\n" +
                "\tUNION ALL\n" +
                "\tSELECT f1\n" +
                "\tFROM t3\n" +
                ")", stmt.toString());

        SQLUnionQuery query = ((SQLUnionQueryTableSource) stmt.getSelect().getQueryBlock().getFrom()).getUnion();

        SQLUnionQuery left = (SQLUnionQuery) query.getLeft();

        SQLSelectQueryBlock leftRight = (SQLSelectQueryBlock) left.getRight();
        leftRight.addOrderBy(new SQLIdentifierExpr("f1"));

        ((SQLSelectQueryBlock) query.getRight()).addOrderBy(new SQLIdentifierExpr("f1"));

        assertEquals("SELECT *\n" +
                "FROM (\n" +
                "\tSELECT f1\n" +
                "\tFROM t1\n" +
                "\tUNION ALL\n" +
                "\t(SELECT f1\n" +
                "\tFROM t2\n" +
                "\tORDER BY f1)\n" +
                "\tUNION ALL\n" +
                "\t(SELECT f1\n" +
                "\tFROM t3\n" +
                "\tORDER BY f1)\n" +
                ")", stmt.toString());
    }
}
