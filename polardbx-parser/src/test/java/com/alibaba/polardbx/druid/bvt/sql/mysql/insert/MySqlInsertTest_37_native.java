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
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

public class MySqlInsertTest_37_native extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "INSERT INTO tbl_name (col1,col2,col3, col4, col5,col6) VALUES(1,'abc',4.5, now(), Date '2018-07-14', TIMESTAMP '2018-05-24 12:12:21');";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.InsertValueNative);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        SQLInsertStatement insertStmt = (SQLInsertStatement) stmt;

        assertEquals(6, insertStmt.getValues().getValues().size());
        assertEquals(6, insertStmt.getColumns().size());
        assertEquals(1, statementList.size());

        List values = ((SQLInsertStatement) stmt).getValues().getValues();
        assertEquals(6, values.size());
        assertEquals(1, values.get(0));
        assertEquals("abc", values.get(1));
        assertEquals(new BigDecimal("4.5"), values.get(2));
        assertEquals(SQLMethodInvokeExpr.class, values.get(3).getClass());
        assertEquals(java.sql.Date.class, values.get(4).getClass());
        assertEquals(com.alibaba.polardbx.druid.sql.ast.expr.SQLTimestampExpr.class, values.get(5).getClass());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        assertEquals("INSERT INTO tbl_name (col1, col2, col3, col4, col5\n" +
                "\t, col6)\n" +
                "VALUES (1, 'abc', 4.5, now(), DATE '2018-07-14'\n" +
                "\t, TIMESTAMP '2018-05-24 12:12:21');", SQLUtils.toMySqlString(insertStmt));
    }

    public void test_1() throws Exception {
        String sql = "INSERT INTO tbl_name (col1,col2,col3, col4, col5,col6) VALUES(1,'abc',4.5, now(), Date '2018-07-14', TIMESTAMP '2018-05-24 12:12:21');";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.InsertValueNative);
        parser.setTimeZone(TimeZone.getDefault());
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        SQLInsertStatement insertStmt = (SQLInsertStatement) stmt;

        assertEquals(6, insertStmt.getValues().getValues().size());
        assertEquals(6, insertStmt.getColumns().size());
        assertEquals(1, statementList.size());

        List values = ((SQLInsertStatement) stmt).getValues().getValues();
        assertEquals(6, values.size());
        assertEquals(1, values.get(0));
        assertEquals("abc", values.get(1));
        assertEquals(new BigDecimal("4.5"), values.get(2));
        assertEquals(java.sql.Timestamp.class, values.get(3).getClass());
        assertEquals(java.sql.Date.class, values.get(4).getClass());
        assertEquals(java.sql.Timestamp.class, values.get(5).getClass());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        java.sql.Timestamp now = parser.getCurrentTimestamp();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String ts = "TIMESTAMP '" + dateFormat.format(now) + "'";

        assertEquals("INSERT INTO tbl_name (col1, col2, col3, col4, col5\n" +
                "\t, col6)\n" +
                "VALUES (1, 'abc', 4.5, " + ts + ", DATE '2018-07-14'\n" +
                "\t, TIMESTAMP '2018-05-24 12:12:21.000');", SQLUtils.toMySqlString(insertStmt));
    }

    public void test_2() throws Exception {
        String sql = "INSERT INTO tbl_name (col1,col2,col3, col4, col5,col6) VALUES(now(), current_timestamp, current_date, sysdate, cur_date, curdate());";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.InsertValueNative, SQLParserFeature.KeepInsertValueClauseOriginalString);
        parser.setTimeZone(TimeZone.getDefault());
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        java.sql.Timestamp now = parser.getCurrentTimestamp();
        java.sql.Date currentDate = parser.getCurrentDate();

        SQLInsertStatement insertStmt = (SQLInsertStatement) stmt;

        assertEquals(6, insertStmt.getValues().getValues().size());
        assertEquals(6, insertStmt.getColumns().size());
        assertEquals(1, statementList.size());

        List values = ((SQLInsertStatement) stmt).getValues().getValues();
        assertEquals(6, values.size());
        assertEquals(java.sql.Timestamp.class, values.get(0).getClass());
        assertEquals(java.sql.Timestamp.class, values.get(1).getClass());
        assertEquals(java.sql.Date.class, values.get(2).getClass());
        assertEquals(java.sql.Timestamp.class, values.get(3).getClass());
        assertEquals(java.sql.Date.class, values.get(4).getClass());
        assertEquals(java.sql.Date.class, values.get(5).getClass());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);


        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String ts = "TIMESTAMP '" + dateFormat.format(now) + "'";
        String ds = "DATE '" + currentDate.toString() + "'";

        assertEquals("INSERT INTO tbl_name (col1, col2, col3, col4, col5\n" +
                "\t, col6)\n" +
                "VALUES (" + ts + ", " + ts + ", " + ds + ", " + ts + ", " + ds + "\n" +
                "\t, " + ds + ");", SQLUtils.toMySqlString(insertStmt));
    }

    public void test_3() throws Exception {
        String sql = "INSERT INTO tbl_name (col1) VALUES(uuid());";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.InsertValueNative, SQLParserFeature.KeepInsertValueClauseOriginalString);
        parser.setTimeZone(TimeZone.getDefault());
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        java.sql.Timestamp now = parser.getCurrentTimestamp();
        java.sql.Date currentDate = parser.getCurrentDate();

        SQLInsertStatement insertStmt = (SQLInsertStatement) stmt;

        assertEquals(1, insertStmt.getValues().getValues().size());
        assertEquals(1, insertStmt.getColumns().size());
        assertEquals(1, statementList.size());

        List values = ((SQLInsertStatement) stmt).getValues().getValues();
        assertEquals(1, values.size());
        assertEquals(String.class, values.get(0).getClass());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);
        assertNull(((SQLInsertStatement) stmt).getValues().getOriginalString());
    }

    public void test_4_error() throws Exception {
        String sql = "INSERT INTO tbl_name (col1, col2) VALUES(1, 5ss);";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.InsertValueNative, SQLParserFeature.KeepInsertValueClauseOriginalString);
        parser.setTimeZone(TimeZone.getDefault());
        Exception error = null;
        try {
            List<SQLStatement> statementList = parser.parseStatementList();
        } catch (ParserException ex) {
            error = ex;
        }
        assertEquals("insert value error, token 5ss, line 1, column 48", error.getMessage());
    }
}
