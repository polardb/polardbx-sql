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

package com.alibaba.polardbx.druid.bvt.sql.mysql.select;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlSelectTest_168_int extends MysqlTest {

    public void test_1() throws Exception {
        String sql = "/*+engine=MPP*/ SELECT  ceil(SMALLINT'123')";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+engine=MPP*/\n" + "SELECT ceil(SMALLINT '123')", stmt.toString());
    }

    public void test_2() throws Exception {
        String sql = "/*+engine=MPP*/ SELECT floor(SMALLINT'123')";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, DbType.mysql, SQLParserFeature.PipesAsConcat);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+engine=MPP*/\n" + "SELECT floor(SMALLINT '123')", stmt.toString());
    }

    public void test_tiny_1() throws Exception {
        String sql = "/*+engine=MPP*/ SELECT  ceil(TINYINT'123')";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+engine=MPP*/\n" + "SELECT ceil(TINYINT '123')", stmt.toString());
    }

    public void test_tiny_2() throws Exception {
        String sql = "/*+engine=MPP*/ SELECT floor(TINYINT'123')";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, DbType.mysql, SQLParserFeature.PipesAsConcat);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+engine=MPP*/\n" + "SELECT floor(TINYINT '123')", stmt.toString());
    }

    public void test_big_1() throws Exception {
        String sql = "/*+engine=MPP*/ SELECT  ceil(BIGINT'123')";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+engine=MPP*/\n" + "SELECT ceil(BIGINT '123')", stmt.toString());
    }

    public void test_big_2() throws Exception {
        String sql = "/*+engine=MPP*/ SELECT floor(BIGINT'123')";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, DbType.mysql, SQLParserFeature.PipesAsConcat);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+engine=MPP*/\n" + "SELECT floor(BIGINT '123')", stmt.toString());
    }
    public void test_real_1() throws Exception {
        String sql = "/*+engine=MPP*/ SELECT floor(REAL '-123.0')";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, DbType.mysql, SQLParserFeature.PipesAsConcat);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+engine=MPP*/\n" + "SELECT floor(REAL '-123.0')", stmt.toString());
    }
    public void test_real_2() throws Exception {
        String sql = "/*+engine=MPP*/ SELECT ceil(REAL '-123.0')";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, DbType.mysql, SQLParserFeature.PipesAsConcat);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+engine=MPP*/\n" + "SELECT ceil(REAL '-123.0')", stmt.toString());
    }
    public void test_double_3() throws Exception {
        String sql = "/*+engine=MPP*/ SELECT floor(CAST(NULL as DOUBLE))";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, DbType.mysql, SQLParserFeature.PipesAsConcat);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+engine=MPP*/\n" + "SELECT floor(CAST(NULL AS DOUBLE))", stmt.toString());
    }
    public void test_double_4() throws Exception {
        String sql = "/*+engine=MPP*/ SELECT floor(CAST(NULL as DECIMAL(25,5)))";
//
        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, DbType.mysql, SQLParserFeature.PipesAsConcat);
        SQLSelectStatement stmt = (SQLSelectStatement)statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+engine=MPP*/\n" + "SELECT floor(CAST(NULL AS DECIMAL(25, 5)))", stmt.toString());
    }


}