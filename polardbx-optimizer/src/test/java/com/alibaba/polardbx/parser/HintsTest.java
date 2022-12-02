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
package com.alibaba.polardbx.parser;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import junit.framework.TestCase;
import org.junit.Assert;

public class HintsTest extends TestCase {
    public void test_hints_0() throws Exception {
        String sql = "CREATE /*!32302 TEMPORARY */ TABLE t (a INT);";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("CREATE /*!32302 TEMPORARY */ TABLE t (\n\ta INT\n);", output);
    }

    public void test_hints_1() throws Exception {
        String sql = "SELECT /*! STRAIGHT_JOIN */ col1 FROM table1,table2";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SELECT /*! STRAIGHT_JOIN */ col1\nFROM table1, table2", output);
    }

    public void test_hints_none() throws Exception {
        String sql = "SELECT /* STRAIGHT_JOIN */ col1 FROM table1,table2";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SELECT col1\nFROM table1, table2", output);
    }

    public void test_hints_head() throws Exception {
        String sql = "/* STRAIGHT_JOIN */ SELECT col1 FROM table1,table2";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("/* STRAIGHT_JOIN */\n" + "SELECT col1\n" + "FROM table1, table2", output);
    }

    public void test_hints_schema_name() throws Exception {
        String sql = HintUtil.buildPushdown("t1", null, "test-schema");
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("SELECT *\n" +
                "FROM `test-schema`.t1", output);
    }
}
