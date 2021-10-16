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

import com.alibaba.polardbx.druid.sql.ast.expr.SQLMatchAgainstExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import junit.framework.TestCase;
import org.junit.Assert;

public class EqualTest_Match_mysql extends TestCase {

    public void test_exits() throws Exception {
        String sql = "MATCH (f1, f2) AGAINST (f3 IN BOOLEAN MODE)";
        String sql_c = "MATCH (f1, f2) AGAINST (f4 IN BOOLEAN MODE)";
        SQLMatchAgainstExpr exprA, exprB, exprC;
        {
            MySqlExprParser parser = new MySqlExprParser(sql);
            exprA = (SQLMatchAgainstExpr) parser.expr();
        }
        {
            MySqlExprParser parser = new MySqlExprParser(sql);
            exprB = (SQLMatchAgainstExpr) parser.expr();
        }
        {
            MySqlExprParser parser = new MySqlExprParser(sql_c);
            exprC = (SQLMatchAgainstExpr) parser.expr();
        }
        Assert.assertEquals(exprA, exprB);
        Assert.assertNotEquals(exprA, exprC);
        Assert.assertTrue(exprA.equals(exprA));
        Assert.assertFalse(exprA.equals(new Object()));
        Assert.assertEquals(exprA.hashCode(), exprB.hashCode());

        Assert.assertEquals(new SQLMatchAgainstExpr(), new SQLMatchAgainstExpr());
        Assert.assertEquals(new SQLMatchAgainstExpr().hashCode(), new SQLMatchAgainstExpr().hashCode());
   
        exprA.setColumns(null);
        exprB.setColumns(null);
        Assert.assertEquals(exprA, exprB);
        Assert.assertNotEquals(exprA, exprC);
        Assert.assertTrue(exprA.equals(exprA));
        Assert.assertFalse(exprA.equals(new Object()));
        Assert.assertEquals(exprA.hashCode(), exprB.hashCode());
    }
}
