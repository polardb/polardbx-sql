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
/**
 * (created at 2011-11-11)
 */
package com.alibaba.polardbx.parser;

import com.alibaba.polardbx.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import junit.framework.TestCase;
import org.junit.Assert;

public class LiteralHexadecimalTest extends TestCase {
    public void test_0() throws Exception {
        String sql = "x'E982B1E7A195275C73'";
        SQLHexExpr hex = (SQLHexExpr) new MySqlExprParser(sql).expr();
        Assert.assertEquals("邱硕'\\s", new String(hex.toBytes(), "utf-8"));
    }

    public void test_1() throws Exception {
        String sql = "x'0D0A'";
        SQLHexExpr hex = (SQLHexExpr) new MySqlExprParser(sql).expr();
        Assert.assertEquals("\r\n", new String(hex.toBytes(), "utf-8"));
    }

    public void test_2() throws Exception {
        String sql = "X'4D7953514C'";
        SQLHexExpr hex = (SQLHexExpr) new MySqlExprParser(sql).expr();
        Assert.assertEquals("MySQL", new String(hex.toBytes(), "utf-8"));
    }

    public void test_3() throws Exception {
        String sql = "0x5061756c";
        SQLHexExpr hex = (SQLHexExpr) new MySqlExprParser(sql).expr();
        Assert.assertEquals("Paul", new String(hex.toBytes(), "utf-8"));
    }

    public void test_4() throws Exception {
        String sql = "0x41";
        SQLHexExpr hex = (SQLHexExpr) new MySqlExprParser(sql).expr();
        Assert.assertEquals("A", new String(hex.toBytes(), "utf-8"));
    }

    public void test_5() throws Exception {
        String sql = "0x636174";
        SQLHexExpr hex = (SQLHexExpr) new MySqlExprParser(sql).expr();
        Assert.assertEquals("cat", new String(hex.toBytes(), "utf-8"));
    }
}
