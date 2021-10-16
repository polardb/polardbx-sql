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
package com.alibaba.polardbx.druid.bvt.sql.mysql.drop;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import org.junit.Assert;

import java.util.List;

public class MySqlDropDatabaseTest extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "DROP DATABASE sonar";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLDropDatabaseStatement stmt = (SQLDropDatabaseStatement)statementList.get(0);

        Assert.assertEquals(1, statementList.size());
        Assert.assertTrue(stmt.isRestrict());
        Assert.assertTrue(!stmt.isCascade());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);
        
        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("DROP DATABASE sonar", //
                            output);

        Assert.assertEquals(0, visitor.getTables().size());
        Assert.assertEquals(0, visitor.getColumns().size());
        Assert.assertEquals(0, visitor.getConditions().size());

    }
    public void test_1() throws Exception {
        String sql = "DROP DATABASE sonar restrict";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLDropDatabaseStatement stmt = (SQLDropDatabaseStatement) statementList.get(0);

        Assert.assertEquals(1, statementList.size());

        Assert.assertTrue(stmt.isRestrict());
        Assert.assertTrue(!stmt.isCascade());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("DROP DATABASE sonar RESTRICT", //
                            output);

    }
    public void test_2() throws Exception {
        String sql = "DROP DATABASE sonar cascade";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLDropDatabaseStatement stmt = (SQLDropDatabaseStatement) statementList.get(0);



        Assert.assertEquals(1, statementList.size());

        Assert.assertTrue(!stmt.isRestrict());
        Assert.assertTrue(stmt.isCascade());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        Assert.assertEquals("DROP DATABASE sonar CASCADE", //
                            output);

    }
}
