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

package com.alibaba.polardbx.druid.bvt.sql.expr;

import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExprGroup;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator;
import junit.framework.TestCase;

public class SQLBinaryOpExprTest extends TestCase {
    public void test_and_0() throws Exception {
        assertNull(SQLBinaryOpExpr.and(null, null));

        SQLBinaryOpExpr c0 = SQLBinaryOpExpr.conditionEq("a", 0);
        assertSame(c0, SQLBinaryOpExpr.and(c0, null));
        assertSame(c0, SQLBinaryOpExpr.and(null, c0));
    }

    public void test_and_1() throws Exception {
        SQLBinaryOpExpr c0 = SQLBinaryOpExpr.conditionEq("a", 0);
        SQLBinaryOpExpr c1 = SQLBinaryOpExpr.conditionEq("a", 1);
        SQLBinaryOpExpr c01 = (SQLBinaryOpExpr) SQLBinaryOpExpr.and(c0, c1);
        assertSame(c0, c01.getLeft());
        assertSame(c1, c01.getRight());
    }

    public void test_and_2() throws Exception {
        SQLBinaryOpExpr c0 = SQLBinaryOpExpr.conditionEq("a", 0);
        SQLBinaryOpExpr c1 = SQLBinaryOpExpr.conditionEq("a", 1);

        SQLBinaryOpExprGroup group = new SQLBinaryOpExprGroup(SQLBinaryOperator.BooleanOr);
        group.add(c1);

        SQLBinaryOpExpr c01 = (SQLBinaryOpExpr) SQLBinaryOpExpr.and(group, c0);
        assertEquals(c1, c01.getLeft());
        assertEquals(c0, c01.getRight());
    }

    public void test_and_3() throws Exception {
        SQLBinaryOpExpr c0 = SQLBinaryOpExpr.conditionEq("a", 0);
        SQLBinaryOpExpr c1 = SQLBinaryOpExpr.conditionEq("a", 1);

        SQLBinaryOpExprGroup group = new SQLBinaryOpExprGroup(SQLBinaryOperator.BooleanOr);
        group.add(c1);

        SQLBinaryOpExpr c01 = (SQLBinaryOpExpr) SQLBinaryOpExpr.and(c0, group);
        assertEquals(c0, c01.getLeft());
        assertEquals(c1, c01.getRight());
    }

    public void test_and_4() throws Exception {
        SQLBinaryOpExpr c0 = SQLBinaryOpExpr.conditionEq("a", 0);
        SQLBinaryOpExpr c1 = SQLBinaryOpExpr.conditionEq("a", 1);
        SQLBinaryOpExpr c2 = SQLBinaryOpExpr.conditionEq("a", 1);

        SQLBinaryOpExprGroup group = new SQLBinaryOpExprGroup(SQLBinaryOperator.BooleanAnd);
        group.add(c1);
        group.add(c2);

        SQLBinaryOpExprGroup c01 = (SQLBinaryOpExprGroup) SQLBinaryOpExpr.and(group, c0);
        assertSame(c01, group);
        assertSame(c1, group.getItems().get(0));
        assertSame(c2, group.getItems().get(1));
        assertSame(c0, group.getItems().get(2));
    }

    public void test_and_5() throws Exception {
        SQLBinaryOpExpr c0 = SQLBinaryOpExpr.conditionEq("a", 0);
        SQLBinaryOpExpr c1 = SQLBinaryOpExpr.conditionEq("a", 1);
        SQLBinaryOpExpr c2 = SQLBinaryOpExpr.conditionEq("a", 1);

        SQLBinaryOpExprGroup group = new SQLBinaryOpExprGroup(SQLBinaryOperator.BooleanAnd);
        group.add(c1);
        group.add(c2);

        SQLBinaryOpExprGroup c01 = (SQLBinaryOpExprGroup) SQLBinaryOpExpr.and(c0, group);
        assertSame(c01, group);
        assertSame(c0, group.getItems().get(0));
        assertSame(c1, group.getItems().get(1));
        assertSame(c2, group.getItems().get(2));
    }
}
