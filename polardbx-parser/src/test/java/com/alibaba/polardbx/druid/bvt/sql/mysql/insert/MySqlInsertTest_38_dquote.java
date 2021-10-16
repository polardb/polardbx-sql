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

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import org.junit.Assert;

import java.util.List;

public class MySqlInsertTest_38_dquote extends MysqlTest {

    public void test_insert_rollback_on_dquote() throws Exception {
        String sql = "insert into \"t\".\"t1\" (\"id\",\"id2\") values (\"1\",uuid());";

        MySqlStatementParser parser = new MySqlStatementParser(sql
                , SQLParserFeature.IgnoreNameQuotes
                , SQLParserFeature.KeepInsertValueClauseOriginalString
                , SQLParserFeature.InsertValueNative);
        parser.setParseCompleteValues(false);
        parser.setParseValuesSize(3);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;

        Assert.assertEquals(1, insertStmt.getValuesList().size());
        Assert.assertEquals(2, insertStmt.getValues().getValues().size());
        Assert.assertEquals(2, insertStmt.getColumns().size());
        Assert.assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

//        System.out.println("Tables : " + visitor.getTables());
//        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("relationships : " + visitor.getRelationships());
//
        assertTrue(visitor.containsTable("t.t1"));

        String formatSql = "INSERT INTO t.t1 (id, id2)\n" +
                "VALUES ('1', ";
        assertTrue(insertStmt.toString().startsWith(formatSql));

        assertNull(insertStmt.getValues().getOriginalString());
    }

    public void test_insert_rollback_on_single_quote() throws Exception {
        String sql = "\n" +
                "INSERT INTO test_realtime1 (\"id\"，\"int_test1\", 'boolean_test', \"byte_test\", \"short_test\", \"int_test2\", \"float_test\", \"string_test\", \"date_test\", \"time_test\", \"timestamp_test\", \"double_test\") \n" +
                "values (6, 6, 0, 6, 6, 6, 6.0, '6', '1990-03-22', '12:23:19', \"1990-03-22 12:23:18\", 1.0);";

        MySqlStatementParser parser = new MySqlStatementParser(sql
                , SQLParserFeature.IgnoreNameQuotes
                , SQLParserFeature.KeepInsertValueClauseOriginalString
                , SQLParserFeature.InsertValueNative);
        parser.setParseCompleteValues(false);
        parser.setParseValuesSize(3);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;

        Assert.assertEquals(1, insertStmt.getValuesList().size());
        Assert.assertEquals(12, insertStmt.getValues().getValues().size());
        Assert.assertEquals(12, insertStmt.getColumns().size());
        Assert.assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

//        System.out.println("Tables : " + visitor.getTables());
//        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("relationships : " + visitor.getRelationships());
//
        assertTrue(visitor.containsTable("test_realtime1"));

        String formatSql = "INSERT INTO test_realtime1 (id, int_test1, boolean_test, byte_test, short_test\n" +
                "\t, int_test2, float_test, string_test, date_test, time_test\n" +
                "\t, timestamp_test, double_test)\n" +
                "VALUES (6, 6, 0, 6, 6\n" +
                "\t, 6, 6.0, '6', '1990-03-22', '12:23:19'\n" +
                "\t, '1990-03-22 12:23:18', 1.0);";
        assertEquals(formatSql, insertStmt.toString());

        assertEquals("(6, 6, 0, 6, 6, 6, 6.0, '6', '1990-03-22', '12:23:19', \"1990-03-22 12:23:18\", 1.0)", insertStmt.getValues().getOriginalString());
    }
}
