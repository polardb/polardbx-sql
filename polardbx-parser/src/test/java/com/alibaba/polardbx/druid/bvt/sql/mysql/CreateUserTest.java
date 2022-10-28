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
package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import org.junit.Assert;

import java.util.List;

public class CreateUserTest extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE USER 'jeffrey'@'localhost' IDENTIFIED BY 'mypass';";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statemen = statementList.get(0);

        Assert.assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        statemen.accept(visitor);

        Assert.assertTrue(visitor.getTables().size() == 0);
    }

    public void test_1() throws Exception {
        String sql = "create user IF NOT EXISTS user1 IDENTIFIED BY 'auth_string'";

        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);

        assertEquals("CREATE USER IF NOT EXISTS 'user1'@'%' IDENTIFIED BY 'auth_string'", statement.toString());
    }

    public void test_2() throws Exception {
        String sql = "create user IF NOT EXISTS user1 IDENTIFIED BY \"auth_string\"";

        SQLStatement statement = SQLUtils.parseSingleMysqlStatement(sql);

        assertEquals("CREATE USER IF NOT EXISTS 'user1'@'%' IDENTIFIED BY 'auth_string'", statement.toString());
    }
}
