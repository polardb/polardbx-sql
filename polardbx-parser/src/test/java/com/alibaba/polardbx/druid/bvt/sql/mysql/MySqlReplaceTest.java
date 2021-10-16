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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;

import java.util.List;

public class MySqlReplaceTest extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "replace into t1 (id,name) values('1','aa'),('2','bb')";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);
        assertEquals("REPLACE INTO t1 (id, name)\n" +
                "VALUES ('1', 'aa'), ('2', 'bb')", SQLUtils.toMySqlString(stmt));
        assertEquals("replace into t1 (id, name)\n" +
                "values ('1', 'aa'), ('2', 'bb')", SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());
        
        assertEquals(1, visitor.getTables().size());
        assertEquals(2, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());

        assertTrue(visitor.containsTable("t1"));

        assertTrue(visitor.containsColumn("t1", "id"));
        assertTrue(visitor.containsColumn("t1", "name"));
    }

    public void test_1() throws Exception {
        String sql = "replace into t1 (id,name) values('1','aa'),('2','bb')";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLReplaceStatement old = (SQLReplaceStatement)statementList.get(0);

        SQLStatement stmt = old.clone();

        assertEquals("REPLACE INTO t1 (id, name)\n" +
                "VALUES ('1', 'aa'), ('2', 'bb')", SQLUtils.toMySqlString(stmt));
        assertEquals("replace into t1 (id, name)\n" +
                "values ('1', 'aa'), ('2', 'bb')", SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(1, visitor.getTables().size());
        assertEquals(2, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());

        assertTrue(visitor.containsTable("t1"));

        assertTrue(visitor.containsColumn("t1", "id"));
        assertTrue(visitor.containsColumn("t1", "name"));
    }

    public void test_2() throws Exception {
        String sql = "replace into student(id,name,unit) value(?,?,?);";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLReplaceStatement old = (SQLReplaceStatement)statementList.get(0);

        SQLStatement stmt = old.clone();

        assertEquals("REPLACE INTO student (id, name, unit)\n" +
                "VALUES (?, ?, ?)", SQLUtils.toMySqlString(stmt));
        assertEquals("replace into student (id, name, unit)\n" +
                "values (?, ?, ?)", SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(1, visitor.getTables().size());
        assertEquals(3, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());

        assertTrue(visitor.containsTable("student"));

        assertTrue(visitor.containsColumn("student", "id"));
        assertTrue(visitor.containsColumn("student", "name"));
    }
}
