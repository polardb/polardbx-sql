/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import junit.framework.TestCase;
import org.junit.Assert;

public class DALLockParserTest extends TestCase {

    public void test_lockTable() throws Exception {
        String sql = "LOCK TABLES t1 READ;";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("LOCK TABLES t1 READ;", output);
    }

    public void test_lockTable_1() throws Exception {
        String sql = "LOCK TABLES t2 READ LOCAL;";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("LOCK TABLES t2 READ LOCAL;", output);
    }

    public void test_lockTable_2() throws Exception {
        String sql = "LOCK TABLES tddl5_users as t LOW_PRIORITY write";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("LOCK TABLES tddl5_users t LOW_PRIORITY WRITE", output);
    }

    public void test_lockTable_3() throws Exception {
        String sql = "LOCK TABLES tddl5_users as t1, table3 as t2 LOW_PRIORITY write";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("LOCK TABLES tddl5_users t1, table3 t2 LOW_PRIORITY WRITE", output);
    }

    public void test_lockTable_4() throws Exception {
        String sql = "LOCK TABLES tddl5_users as t1 read, table3 as t2 LOW_PRIORITY write";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("LOCK TABLES tddl5_users t1 READ, table3 t2 LOW_PRIORITY WRITE", output);
    }

    public void test_unlockTable() throws Exception {
        String sql = "UNLOCK TABLES";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("UNLOCK TABLES", output);
    }
}
