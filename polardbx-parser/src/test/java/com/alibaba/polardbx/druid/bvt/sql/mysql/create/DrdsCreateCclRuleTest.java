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

package com.alibaba.polardbx.druid.bvt.sql.mysql.create;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import org.junit.Test;

/**
 * @author busu
 */
public class DrdsCreateCclRuleTest extends MysqlTest {

    @Test
    public void test1() {

        final String[] queryTypes = new String[] {"UPDATE", "SELECT", "INSERT", "DELETE"};

        for (String queryType : queryTypes) {
            String sql = String.format("CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' \n"
                + "FOR %s\n"
                + "FILTER BY KEYWORD('keyword1', 'keyword2', 'keyword3')\n"
                + "FILTER BY TEMPLATE 'template_id' "
                + "WITH MAX_CONCURRENCY=500, WAIT_QUEUE_SIZE = 10", queryType);
            MySqlStatementParser parser = newParser(sql);
            SQLStatement stmt = parser.parseStatement();
            String output = SQLUtils.toMySqlString(stmt);
            System.out.println(output);
            assertEquals(String.format(
                "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' FOR %s FILTER BY KEYWORD ('keyword1', 'keyword2', 'keyword3') FILTER BY TEMPLATE ('template_id') WITH MAX_CONCURRENCY = 500, WAIT_QUEUE_SIZE = 10",
                queryType), output);

        }

    }

    @Test
    public void test2() {
        String sql = "CREATE CCL_RULE busu ON busudb TO 'dingfeng'@'127.0.0.1' \n"
            + "FOR UPDATE\n"
            + "FILTER BY KEYWORD('keyword1', 'keyword2', 'keyword3')\n"
            + "FILTER BY TEMPLATE 'template_id' \n"
            + "WITH MAX_CONCURRENCY=500, WAIT_QUEUE_SIZE = 10";
        MySqlStatementParser parser = newParser(sql);
        String exceptionMessage = null;
        try {
            SQLStatement stmt = parser.parseStatement();
        } catch (ParserException parserException) {
            exceptionMessage = parserException.getMessage();
        }
        System.out.println(exceptionMessage);
        assertEquals("syntax error, no table, pos 33, line 1, column 32, token TO", exceptionMessage);
    }

    @Test
    public void test3() {
        String sql = "CREATE CCL_RULE busu ON busutb";
        MySqlStatementParser parser = newParser(sql);
        String exceptionMessage = null;
        try {
            SQLStatement stmt = parser.parseStatement();
        } catch (ParserException parserException) {
            exceptionMessage = parserException.getMessage();
        }
        System.out.println(exceptionMessage);
        assertEquals("syntax error, no table, pos 30, line 1, column 31, token EOF", exceptionMessage);
    }

    @Test
    public void test4() {
        String sql = "CREATE CCL_RULE busu ON busudb.busutb TO  'dingfeng'";
        MySqlStatementParser parser = newParser(sql);
        String exceptionMessage = null;
        try {
            SQLStatement stmt = parser.parseStatement();
        } catch (ParserException parserException) {
            exceptionMessage = parserException.getMessage();
        }
        System.out.println(exceptionMessage);
        assertEquals("syntax error, no user, example: 'username'@'host', pos 52, line 1, column 53, token EOF",
            exceptionMessage);
    }

    @Test
    public void test5() {
        String sql = "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' \n"
            + "FOR UPDATE\n"
            + "WITH MAX_CONCURRENCY=500, WAIT_QUEUE_SIZE = 10";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        assertEquals(
            "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' FOR UPDATE WITH MAX_CONCURRENCY = 500, WAIT_QUEUE_SIZE = 10",
            output);
    }

    @Test
    public void test6() {
        String sql = "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' \n"
            + "FOR UPDATE\n"
            + " FILTER BY TEMPLATE 'template_id'\n"
            + " WITH MAX_CONCURRENCY=500, WAIT_QUEUE_SIZE = 10";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        assertEquals(
            "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' FOR UPDATE FILTER BY TEMPLATE ('template_id') WITH MAX_CONCURRENCY = 500, WAIT_QUEUE_SIZE = 10",
            output);
    }

    @Test
    public void test7() {
        String sql = "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' \n"
            + "FOR UPDATE\n"
            + " FILTER BY TEMPLATE 'template_id'\n"
            + " WITH MAX_CONCURRENCY=500";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        assertEquals(
            "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' FOR UPDATE FILTER BY TEMPLATE ('template_id') WITH MAX_CONCURRENCY = 500",
            output);
    }

    @Test
    public void test8() {
        String sql = "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' \n"
            + "FOR UPDATE\n"
            + " FILTER BY TEMPLATE 'template_id'\n"
            + " WITH MAX_CONCURRENCY=500;";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        assertEquals(
            "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' FOR UPDATE FILTER BY TEMPLATE ('template_id') WITH MAX_CONCURRENCY = 500",
            output);
    }

    @Test
    public void test9() {
        String sql = "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' \n"
            + "FOR UPDATE\n"
            + " FILTER BY TEMPLATE 'template_id'\n"
            + " WITH MAX_CONCURRENCY=500;";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        assertEquals(
            "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' FOR UPDATE FILTER BY TEMPLATE ('template_id') WITH MAX_CONCURRENCY = 500",
            output);
    }

    @Test
    public void test10() {
        String sql = "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' \n"
            + "FOR UPDATE\n"
            + " FILTER BY  'template_id'\n"
            + " WITH MAX_CONCURRENCY=500;";
        MySqlStatementParser parser = newParser(sql);
        String exceptionMessage = null;
        try {
            SQLStatement stmt = parser.parseStatement();
        } catch (ParserException parserException) {
            exceptionMessage = parserException.getMessage();
        }
        System.out.println(exceptionMessage);
        assertEquals("syntax error, error in :'UPDATE\n"
                + " FILTER BY  'template_id'\n"
                + " WITH MAX_CO', expect WITH, actual null, pos 101, line 3, column 14, token LITERAL_CHARS template_id",
            exceptionMessage);
    }

    @Test
    public void test11() {
        String sql = "CREATE CCL_RULE if not exists busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' \n"
            + "FOR UPDATE\n"
            + " FILTER BY TEMPLATE 'template_id'\n"
            + " WITH MAX_CONCURRENCY=500;";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        assertEquals(
            "CREATE CCL_RULE IF NOT EXISTS busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' FOR UPDATE FILTER BY TEMPLATE ('template_id') WITH MAX_CONCURRENCY = 500",
            output);
    }

    @Test
    public void test12() {
        String sql = "CREATE CCL_RULE if not exists `busu` ON busudb.busutb TO 'dingfeng'@'127.0.0.1' \n"
            + "FOR UPDATE\n"
            + " FILTER BY TEMPLATE 'template_id'\n"
            + " WITH MAX_CONCURRENCY=500;";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        assertEquals(
            "CREATE CCL_RULE IF NOT EXISTS `busu` ON busudb.busutb TO 'dingfeng'@'127.0.0.1' FOR UPDATE FILTER BY TEMPLATE ('template_id') WITH MAX_CONCURRENCY = 500",
            output);
    }

    @Test
    public void test13() {
        String sql = "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' \n"
            + "FOR UPDATE\n"
            + "FILTER BY KEYWORD('keyword1')\n"
            + " WITH MAX_CONCURRENCY=500, WAIT_QUEUE_SIZE = 10";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        assertEquals(
            "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' FOR UPDATE FILTER BY KEYWORD ('keyword1') WITH MAX_CONCURRENCY = 500, WAIT_QUEUE_SIZE = 10",
            output);
    }

    @Test
    public void testFilterByQuery() {
        String sql = "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' \n"
            + "FOR UPDATE\n"
            + "FILTER BY QUERY 'select * from busu where name = \"dingfeng\"'\n"
            + " WITH MAX_CONCURRENCY=500, WAIT_QUEUE_SIZE = 10";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        assertEquals(
            "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' FOR UPDATE FILTER BY QUERY 'select * from busu where name = \"dingfeng\"' WITH MAX_CONCURRENCY = 500, WAIT_QUEUE_SIZE = 10",
            output);
    }

    @Test
    public void testFilterByTemplates() {
        String sql = "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' \n"
            + "FOR UPDATE\n"
            + "FILTER BY TEMPLATE('busutmp1','busutmp2') \n"
            + " WITH MAX_CONCURRENCY=500, WAIT_QUEUE_SIZE = 10";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        assertEquals(
            "CREATE CCL_RULE busu ON busudb.busutb TO 'dingfeng'@'127.0.0.1' FOR UPDATE FILTER BY TEMPLATE ('busutmp1', 'busutmp2') WITH MAX_CONCURRENCY = 500, WAIT_QUEUE_SIZE = 10",
            output);
    }

    @Test
    public void testNoUser() {
        String sql = "CREATE CCL_RULE busu ON busudb.busutb  \n"
            + "FOR UPDATE\n"
            + "FILTER BY TEMPLATE('busutmp1','busutmp2') \n"
            + " WITH MAX_CONCURRENCY=500, WAIT_QUEUE_SIZE = 10";
        MySqlStatementParser parser = newParser(sql);
        SQLStatement stmt = parser.parseStatement();
        String output = SQLUtils.toMySqlString(stmt);
        System.out.println(output);
        assertEquals(
            "CREATE CCL_RULE busu ON busudb.busutb FOR UPDATE FILTER BY TEMPLATE ('busutmp1', 'busutmp2') WITH MAX_CONCURRENCY = 500, WAIT_QUEUE_SIZE = 10",
            output);
    }

    private MySqlStatementParser newParser(String sql) {
        return new MySqlStatementParser(sql, SQLParserFeature.DrdsCCL);
    }

}
