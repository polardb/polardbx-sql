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
package com.alibaba.polardbx.druid.bvt.sql.mysql.insert;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import junit.framework.TestCase;

import java.util.List;

public class MySqlInsertTest_39 extends TestCase {

    public void test_insert_0() throws Exception {
        String sql = "insert  into t1 values('\\\\abc');";

        MySqlStatementParser parser = new MySqlStatementParser(sql, false, true);
        parser.config(SQLParserFeature.KeepInsertValueClauseOriginalString, true);

        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;

        assertEquals(1, insertStmt.getValuesList().size());
        assertEquals(1, insertStmt.getValues().getValues().size());
        assertEquals(0, insertStmt.getColumns().size());
        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor v = new MySqlSchemaStatVisitor();
        stmt.accept(v);

        assertEquals("INSERT INTO t1\n" +
                "VALUES ('\\\\abc');", SQLUtils.toMySqlString(insertStmt));

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, DbType.mysql, VisitorFeature.OutputParameterizedQuesUnMergeValuesList);
        assertEquals("INSERT INTO t1\n" +
                "VALUES (?);", psql);


//                System.out.println("Tables : " + visitor.getTables());
//        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(1, v.getTables().size());
//        assertEquals(1, visitor.getColumns().size());
//        assertEquals(0, visitor.getConditions().size());
//        assertEquals(0, visitor.getOrderByColumns().size());

        System.out.println(sql);
        System.out.println(stmt.toString());

    }

    public void test_insert_1() throws Exception {
        String sql = "insert into t1 values(  \"\\\\abc\")";

        MySqlStatementParser parser = new MySqlStatementParser(sql, false, true);
        parser.config(SQLParserFeature.KeepInsertValueClauseOriginalString, true);

        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;

        assertEquals(1, insertStmt.getValuesList().size());
        assertEquals(1, insertStmt.getValues().getValues().size());
        assertEquals(0, insertStmt.getColumns().size());
        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor v = new MySqlSchemaStatVisitor();
        stmt.accept(v);

        assertEquals("INSERT INTO t1\n" +
                "VALUES ('\\\\abc')", SQLUtils.toMySqlString(insertStmt));

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, DbType.mysql, VisitorFeature.OutputParameterizedQuesUnMergeValuesList);
        assertEquals("INSERT INTO t1\n" +
                "VALUES (?)", psql);


//                System.out.println("Tables : " + visitor.getTables());
//        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(1, v.getTables().size());
//        assertEquals(1, visitor.getColumns().size());
//        assertEquals(0, visitor.getConditions().size());
//        assertEquals(0, visitor.getOrderByColumns().size());

        System.out.println(sql);
        System.out.println(stmt.toString());

    }
}
