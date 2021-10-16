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

import com.alibaba.polardbx.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.SQLExprParser;
import junit.framework.TestCase;
import org.junit.Assert;

public class EqualTest_boolean extends TestCase {

    public void test_exits() throws Exception {
        String sql = "true";
        String sql_c = "false";
        SQLBooleanExpr exprA, exprB, exprC;
        {
            SQLExprParser parser = new MySqlExprParser(sql);
            exprA = (SQLBooleanExpr) parser.expr();
        }
        {
            SQLExprParser parser = new MySqlExprParser(sql);
            exprB = (SQLBooleanExpr) parser.expr();
        }
        {
            SQLExprParser parser = new MySqlExprParser(sql_c);
            exprC = (SQLBooleanExpr) parser.expr();
        }
        Assert.assertEquals(exprA, exprB);
        Assert.assertNotEquals(exprA, exprC);
        Assert.assertTrue(exprA.equals(exprA));
        Assert.assertFalse(exprA.equals(new Object()));
        Assert.assertEquals(exprA.hashCode(), exprB.hashCode());
        
        Assert.assertEquals(new SQLBooleanExpr(), new SQLBooleanExpr());
        Assert.assertEquals(new SQLBooleanExpr().hashCode(), new SQLBooleanExpr().hashCode());
    }
}
