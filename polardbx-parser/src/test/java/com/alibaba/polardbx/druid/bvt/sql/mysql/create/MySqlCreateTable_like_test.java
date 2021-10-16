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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableLike;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlCreateTable_like_test extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE like_test (LIKE t1)";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, true);

        assertEquals(1, statementList.size());

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        SQLExprTableSource like = stmt.getLike();
        assertTrue(stmt.getTableElementList().size() == 1);
        assertTrue(like == null);
        assertTrue(stmt.getTableElementList().get(0) instanceof SQLTableLike);
        assertEquals("CREATE TABLE like_test (\n" +
                "\tLIKE t1\n" +
                ")", stmt.toString());
    }


    public void test_1() throws Exception {
        String sql = "CREATE TABLE like_test (`LIKE` t1)";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, true);

        assertEquals(1, statementList.size());

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        assertTrue(stmt.getLike() == null);

        assertEquals("CREATE TABLE like_test (\n" +
                "\t`LIKE` t1\n" +
                ")", stmt.toString());
    }

    public void test_2() throws Exception {
        String sql = "CREATE TABLE like_test LIKE t1";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, true);

        assertEquals(1, statementList.size());

        MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        assertTrue(stmt.getLike() != null);

        assertEquals("CREATE TABLE like_test LIKE t1", stmt.toString());
    }
}
