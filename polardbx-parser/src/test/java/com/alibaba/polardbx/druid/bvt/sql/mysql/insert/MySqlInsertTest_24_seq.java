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

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;

import java.util.ArrayList;
import java.util.List;

public class MySqlInsertTest_24_seq extends MysqlTest {

    public void test_insert() throws Exception {
        String sql = "INSERT INTO abc VALUES(myseq.nextVal, 2, true),(2,3, false);";

        {
            List<Object> outParameters = new ArrayList<Object>();
            String psql = ParameterizedOutputVisitorUtils.parameterize(sql, DbType.mysql, outParameters);
            assertEquals("INSERT INTO abc\n" +
                    "VALUES (myseq.NEXTVAL, ?, ?),\n" +
                    "\t(?, ?, ?);", psql);

            assertEquals(5, outParameters.size());

            String rsql = ParameterizedOutputVisitorUtils.restore(psql, DbType.mysql, outParameters);
            assertEquals("INSERT INTO abc\n" +
                    "VALUES (myseq.NEXTVAL, 2, true),\n" +
                    "\t(2, 3, false);", rsql);
        }

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;

        assertEquals("INSERT INTO abc\n" +
                "VALUES (myseq.NEXTVAL, 2, true),\n" +
                "\t(2, 3, false);", SQLUtils.toMySqlString(insertStmt));


    }

}
