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

import com.alibaba.polardbx.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import junit.framework.TestCase;
import org.junit.Assert;

public class EqualTest_inquery extends TestCase {

    public void test_exits() throws Exception {
        String sql = "fstate in (select state from t_status)";
        String sql_c = "fstate_c in (select state from t_status)";
        SQLInSubQueryExpr exprA, exprB, exprC;
        {
            MySqlExprParser parser = new MySqlExprParser(sql);
            exprA = (SQLInSubQueryExpr) parser.expr();
        }
        {
            MySqlExprParser parser = new MySqlExprParser(sql);
            exprB = (SQLInSubQueryExpr) parser.expr();
        }
        {
            MySqlExprParser parser = new MySqlExprParser(sql_c);
            exprC = (SQLInSubQueryExpr) parser.expr();
        }
        Assert.assertEquals(exprA, exprB);
        Assert.assertNotEquals(exprA, exprC);
        Assert.assertTrue(exprA.equals(exprA));
        Assert.assertFalse(exprA.equals(new Object()));
        Assert.assertEquals(exprA.hashCode(), exprB.hashCode());
    }
}
