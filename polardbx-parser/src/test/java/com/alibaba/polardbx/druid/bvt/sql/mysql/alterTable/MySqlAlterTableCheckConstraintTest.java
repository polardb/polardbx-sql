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
package com.alibaba.polardbx.druid.bvt.sql.mysql.alterTable;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.Token;
import junit.framework.TestCase;
import org.junit.Assert;

public class MySqlAlterTableCheckConstraintTest extends TestCase {

    public void test_alter_add_check() throws Exception {
        String sql = "ALTER TABLE icp.wx_msg ADD check(ID>10)";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);
        
        Assert.assertEquals("ALTER TABLE icp.wx_msg"
                + "\n\tADD CHECK (ID > 10)", SQLUtils.toMySqlString(stmt));

    }

    public void test_alter_add_check_1() throws Exception {
        String sql = "ALTER TABLE icp.wx_msg ADD CONSTRAINT syb check(ID>10)";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);

        Assert.assertEquals("ALTER TABLE icp.wx_msg"
            + "\n\tADD CONSTRAINT syb CHECK (ID > 10)", SQLUtils.toMySqlString(stmt));
    }

    public void test_alter_add_check_2() throws Exception {
        String sql = "ALTER TABLE icp.wx_msg ADD CONSTRAINT check(ID>10) enforced";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);

        Assert.assertEquals("ALTER TABLE icp.wx_msg"
            + "\n\tADD CHECK (ID > 10) ENFORCED", SQLUtils.toMySqlString(stmt));
    }

    public void test_alter_add_check_3() throws Exception {
        String sql = "ALTER TABLE icp.wx_msg ADD CONSTRAINT syb check(ID>10) not enforced";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);

        Assert.assertEquals("ALTER TABLE icp.wx_msg"
            + "\n\tADD CONSTRAINT syb CHECK (ID > 10) NOT ENFORCED", SQLUtils.toMySqlString(stmt));
    }

    public void test_alter_drop_check() throws Exception {
        String sql = "ALTER TABLE icp.wx_msg drop check ic_check";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);

        Assert.assertEquals("ALTER TABLE icp.wx_msg"
            + "\n\t DROP CHECK ic_check", SQLUtils.toMySqlString(stmt));

    }

    public void test_alter_enforced_check() throws Exception {
        String sql = "ALTER TABLE icp.wx_msg ALTER check ic_check ENFORCED";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);

        Assert.assertEquals("ALTER TABLE icp.wx_msg"
            + "\n\t ALTER CHECK ic_check ENFORCED ", SQLUtils.toMySqlString(stmt));
    }

    public void test_alter_not_enforced_check() throws Exception {
        String sql = "ALTER TABLE icp.wx_msg ALTER check ic_check not ENFORCED";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        parser.match(Token.EOF);

        Assert.assertEquals("ALTER TABLE icp.wx_msg"
            + "\n\t ALTER CHECK ic_check NOT ENFORCED ", SQLUtils.toMySqlString(stmt));
    }

}
