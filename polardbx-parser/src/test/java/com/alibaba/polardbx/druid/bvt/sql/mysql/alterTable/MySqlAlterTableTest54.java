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

package com.alibaba.polardbx.druid.bvt.sql.mysql.alterTable;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import junit.framework.TestCase;

public class MySqlAlterTableTest54 extends TestCase {
    public void test_0() throws Exception {
        String sql = "alter table event_log storage_policy = 'HOT'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLAlterTableStatement stmt = (SQLAlterTableStatement) parser.parseStatementList().get(0);
        assertEquals(1, stmt.getTableOptions().size());
        String formatSql = SQLUtils.toSQLString(stmt);
        assertEquals("ALTER TABLE event_log\n" +
                "\tSTORAGE_POLICY = 'HOT'", formatSql);
    }

    public void test_1() throws Exception {
        String sql = "alter table event_log storage_policy = 'COLD'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement stmt = parser.parseStatementList().get(0);
        String formatSql = SQLUtils.toSQLString(stmt);
        assertEquals("ALTER TABLE event_log\n" +
                "\tSTORAGE_POLICY = 'COLD'", formatSql);
    }

    public void test_2() throws Exception {
        String sql = "alter table event_log storage_policy = 'MIXED' hot_partition_count = 10;";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLAlterTableStatement stmt = (SQLAlterTableStatement)parser.parseStatementList().get(0);
        assertEquals(2, stmt.getTableOptions().size());
        String formatSql = SQLUtils.toSQLString(stmt);
        assertEquals("ALTER TABLE event_log\n" +
                "\tSTORAGE_POLICY = 'MIXED' HOT_PARTITION_COUNT = 10;", formatSql);
    }

    public void test_3() throws Exception {
        String sql = "alter table event_log hot_partition_count = 10;";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLAlterTableStatement stmt = (SQLAlterTableStatement) parser.parseStatementList().get(0);
        assertEquals(1, stmt.getTableOptions().size());
        String formatSql = SQLUtils.toSQLString(stmt);
        assertEquals("ALTER TABLE event_log\n" +
                "\tHOT_PARTITION_COUNT = 10;", formatSql);
    }

    public void test_4() throws Exception {
        String sql = "alter table event_log hot_partition_count = 'abc';";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            SQLStatement stmt = parser.parseStatementList().get(0);
            fail();
        } catch (ParserException e) {
            //do nothing
        }
    }

    public void test_5() throws Exception {
        String sql = "alter table event_log hot_partition_count = '10';";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            SQLStatement stmt = parser.parseStatementList().get(0);
            fail();
        } catch (ParserException e) {
            //do nothing
        }
    }

    public void test_6() throws Exception {
        String sql = "alter table event_log storage_policy = HOT";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            SQLAlterTableStatement stmt = (SQLAlterTableStatement) parser.parseStatementList().get(0);
            fail();
        } catch (Exception e) {
        }
    }

    public void test_7() throws Exception {
        String sql = "alter table event_log storage_policy = COLD";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        try {
            SQLAlterTableStatement stmt = (SQLAlterTableStatement) parser.parseStatementList().get(0);
            fail();
        } catch (Exception e) {
        }
    }

    public void test_8() throws Exception {
        String sql = "alter table event_log storage_policy = \"COLD\"";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLAlterTableStatement stmt = (SQLAlterTableStatement) parser.parseStatementList().get(0);
        String formatSql = SQLUtils.toSQLString(stmt);
        assertEquals("ALTER TABLE event_log\n" +
                "\tSTORAGE_POLICY = 'COLD'", formatSql);
    }

    public void test_9() throws Exception {
        String sql = "alter table event_log storage_policy = \"HOT\"";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLAlterTableStatement stmt = (SQLAlterTableStatement) parser.parseStatementList().get(0);
        String formatSql = SQLUtils.toSQLString(stmt);
        assertEquals("ALTER TABLE event_log\n" +
                "\tSTORAGE_POLICY = 'HOT'", formatSql);
    }
}
