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

public class CALL_Test extends TestCase {

    public void test_0() throws Exception {
        String sql = "CALL p(@version, @increment);";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("CALL p(@version, @increment);", text);
    }

    public void test_00() throws Exception {
        String sql = "/*+TDDL:node(0)*/ call simpleproc(@a);";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("/*+TDDL:node(0)*/\nCALL simpleproc(@a);", text);
    }

    public void test_1() throws Exception {
        String sql = "PREPARE s FROM 'CALL p(?, ?)';";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        String text = output(stmtList);

        Assert.assertEquals("PREPARE s FROM 'CALL p(?, ?)';", text);
    }

    public void test_2() throws Exception {
        String sql = "EXECUTE s USING @version, @increment;";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement stmt = stmtList.get(0);

        assertEquals("EXECUTE s USING @version, @increment;", SQLUtils.toMySqlString(stmt));
        assertEquals("execute s using @version, @increment;", SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void test_3() throws Exception {
        String sql = "EXECUTE s";

        SQLStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement stmt = stmtList.get(0);

        assertEquals("EXECUTE s", SQLUtils.toMySqlString(stmt));
        assertEquals("execute s", SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    private String output(List<SQLStatement> stmtList) {
        return SQLUtils.toSQLString(stmtList, JdbcConstants.MYSQL);
    }
}
