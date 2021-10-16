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
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;

import java.util.ArrayList;
import java.util.List;

public class MySqlInsertTest_22 extends MysqlTest {
    String sql = "INSERT INTO abc VALUES(1, 2, true),(2,3, false);";

    public void test_insert_0() throws Exception {
        List<Object> outParameters = new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, DbType.mysql, outParameters, VisitorFeature.OutputParameterizedQuesUnMergeValuesList);
        assertEquals("INSERT INTO abc\n" +
                "VALUES (?, ?, ?),\n" +
                "\t(?, ?, ?);", psql);

        String rsql = ParameterizedOutputVisitorUtils.restore(psql, DbType.mysql, outParameters);
        assertEquals("INSERT INTO abc\n" +
                "VALUES (1, 2, true),\n" +
                "\t(2, 3, false);", rsql);
    }

    public void test_insert_1() throws Exception {
        List<Object> outParameters = new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, DbType.mysql, outParameters);
        assertEquals("INSERT INTO abc\n" +
                "VALUES (?, ?, ?);", psql);

        assertEquals(2, outParameters.size());
        assertEquals(3, ((List<Object>) outParameters.get(0)).size());
        assertEquals(3, ((List<Object>) outParameters.get(1)).size());

        assertEquals(1, ((List<Object>) outParameters.get(0)).get(0));
        assertEquals(2, ((List<Object>) outParameters.get(0)).get(1));
        assertEquals(true, ((List<Object>) outParameters.get(0)).get(2));

        assertEquals(2, ((List<Object>) outParameters.get(1)).get(0));
        assertEquals(3, ((List<Object>) outParameters.get(1)).get(1));
        assertEquals(false, ((List<Object>) outParameters.get(1)).get(2));

        String rsql = ParameterizedOutputVisitorUtils.restore(psql, DbType.mysql, outParameters);
        assertEquals("INSERT INTO abc\n" +
                "VALUES (1, 2, true),\n" +
                "\t(2, 3, false);", rsql);
    }

    public void test_insert() throws Exception {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;

        assertEquals("INSERT INTO abc\n" +
                "VALUES (1, 2, true),\n" +
                "\t(2, 3, false);", SQLUtils.toMySqlString(insertStmt));


    }

}
