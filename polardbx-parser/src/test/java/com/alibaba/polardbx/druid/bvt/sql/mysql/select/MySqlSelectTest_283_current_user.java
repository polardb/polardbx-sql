/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *http://www.apache.org/licenses/LICENSE-2.0
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
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;


public class MySqlSelectTest_283_current_user extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "SELECT current_user from t where u = CURRENT_USER";

        SQLStatement stmt = SQLUtils
                .parseSingleStatement(sql, DbType.mysql, SQLParserFeature.EnableCurrentUserExpr);

        assertEquals("SELECT CURRENT_USER\n" +
                "FROM t\n" +
                "WHERE u = CURRENT_USER", stmt.toString());

        assertEquals("select current_user\n" +
                "from t\n" +
                "where u = current_user", stmt.toLowerCaseString());
    }

    public void test_2() throws Exception {
        String sql = "insert into t1 values(current_user());";

        SQLStatement stmt = SQLUtils
            .parseSingleStatement(sql, DbType.mysql, SQLParserFeature.EnableCurrentUserExpr);

        assertEquals("INSERT INTO t1\n"
            + "VALUES (current_user());", stmt.toString());

        assertEquals("insert into t1\n"
            + "values (current_user());", stmt.toLowerCaseString());
    }

    public void test_3() throws Exception {
        String sql = "insert into t1 values(current_user);";

        SQLStatement stmt = SQLUtils
            .parseSingleStatement(sql, DbType.mysql, SQLParserFeature.EnableCurrentUserExpr);

        assertEquals("INSERT INTO t1\n"
            + "VALUES (CURRENT_USER);", stmt.toString());

        assertEquals("insert into t1\n"
            + "values (current_user);", stmt.toLowerCaseString());
    }

}