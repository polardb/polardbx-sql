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
package com.alibaba.polardbx.druid.bvt.sql.mysql.select;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;

import java.util.List;

public class MySqlSelectTest_fetch_first_rows_only extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "select * from t fetch first 1 row only;";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals("SELECT *\n"
            + "FROM t\n"
            + "LIMIT 1;", output);
    }

    public void test_1() throws Exception {
        String sql = "select * from t order by id fetch first 10 rows only;";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals("SELECT *\n"
            + "FROM t\n"
            + "ORDER BY id\n"
            + "LIMIT 10;", output);
    }

    public void test_2() throws Exception {
        String sql = "select * from t order by id fetch first row only;";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals("SELECT *\n"
            + "FROM t\n"
            + "ORDER BY id\n"
            + "LIMIT 1;", output);
    }

    public void test_3() throws Exception {
        String sql = "select * from t fetch first rows only;";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals("SELECT *\n"
            + "FROM t\n"
            + "LIMIT 1;", output);
    }
}
