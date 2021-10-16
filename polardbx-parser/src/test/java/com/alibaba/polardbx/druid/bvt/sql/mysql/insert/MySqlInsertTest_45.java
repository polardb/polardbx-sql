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

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import junit.framework.TestCase;

import java.util.List;

public class MySqlInsertTest_45 extends TestCase {

    public void test_insert_0() throws Exception {
        String sql = "INSERT INTO table_name(c1)\n" +
                "(select c1 from t1 left join t2 on t1.id = t2.id and t1.a = t2.a)\n" +
                "ON DUPLICATE KEY UPDATE total = VALUES(total) + 1";

        MySqlStatementParser parser = new MySqlStatementParser(sql, false, true);
        parser.config(SQLParserFeature.KeepInsertValueClauseOriginalString, true);

        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;
        assertEquals("INSERT INTO table_name (c1)\n" +
                "SELECT c1\n" +
                "FROM t1\n" +
                "\tLEFT JOIN t2\n" +
                "\tON t1.id = t2.id\n" +
                "\t\tAND t1.a = t2.a\n" +
                "ON DUPLICATE KEY UPDATE total = VALUES(total) + 1", insertStmt.toString());

    }

}
