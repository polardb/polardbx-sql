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

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.parser.SQLExprParser;
import junit.framework.TestCase;

import java.util.List;

/**
 * Created by wenshao on 04/06/2017.
 */
public class SplitTest2 extends TestCase {
    public void test_0() throws Exception {
        int count = 10;
        String sql = "";

        for (int i = 0; i < count; ++i) {
            if (sql.length() != 0) {
                sql += " + ";
            }

            sql += Integer.toString(i);
        }
//        sql = "(1 + 2) + (3 + 4)";
        SQLBinaryOpExpr expr = (SQLBinaryOpExpr) new SQLExprParser(sql).expr();

        List<SQLExpr> items = SQLBinaryOpExpr.split(expr);

        System.out.println(sql);
        System.out.println(items);

        assertEquals(count, items.size());
        for (int i = 0; i < count; ++i) {
            SQLExpr item = items.get(i);
            assertEquals(Integer.toString(i ), SQLUtils.toSQLString(item));
        }
    }
}
