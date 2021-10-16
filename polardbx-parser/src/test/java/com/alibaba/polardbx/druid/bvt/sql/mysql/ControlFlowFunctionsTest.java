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

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.List;

public class ControlFlowFunctionsTest extends TestCase {

    public void test_0() throws Exception {
        String sql = "SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'more' END;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("SELECT CASE 1\n" +
                "\t\tWHEN 1 THEN 'one'\n" +
                "\t\tWHEN 2 THEN 'two'\n" +
                "\t\tELSE 'more'\n" +
                "\tEND;", text);
    }

    public void test_1() throws Exception {
        String sql = "SELECT IF(1>2,2,3);";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("SELECT IF(1 > 2, 2, 3);", text);
    }

    public void test_2() throws Exception {
        String sql = "SELECT IF(1<2,'yes','no');";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("SELECT IF(1 < 2, 'yes', 'no');", text);
    }

    public void test_3() throws Exception {
        String sql = "SELECT IF(STRCMP('test','test1'),'no','yes');";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("SELECT IF(STRCMP('test', 'test1'), 'no', 'yes');", text);
    }

    public void test_4() throws Exception {
        String sql = "SELECT IFNULL(1,0);";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("SELECT IFNULL(1, 0);", text);
    }

    public void test_5() throws Exception {
        String sql = "SELECT IFNULL(1/0,'yes');";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("SELECT IFNULL(1 / 0, 'yes');", text);
    }

    private String output(List<SQLStatement> stmtList) {
        return SQLUtils.toSQLString(stmtList, JdbcConstants.MYSQL);
    }
}
