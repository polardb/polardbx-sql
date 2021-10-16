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

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlInsertTest_47_with_cte extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "insert into table1 with tt as (select * from t) select * from tt;";


        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, true);
        SQLStatement stmt = statementList.get(0);

        assertEquals(1, statementList.size());

        SchemaStatVisitor visitor = SQLUtils.createSchemaStatVisitor(JdbcConstants.MYSQL);
        stmt.accept(visitor);

        assertEquals(2, visitor.getTables().size());
        assertEquals(1, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());
        assertEquals(0, visitor.getOrderByColumns().size());
        
        {
            String output = SQLUtils.toMySqlString(stmt);
            assertEquals("INSERT INTO table1\n" +
                            "WITH tt AS (\n" +
                            "\t\tSELECT *\n" +
                            "\t\tFROM t\n" +
                            "\t)\n" +
                            "SELECT *\n" +
                            "FROM tt;", //
                                output);
        }
        {
            String output = SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION);
            assertEquals("insert into table1\n" +
                            "with tt as (\n" +
                            "\t\tselect *\n" +
                            "\t\tfrom t\n" +
                            "\t)\n" +
                            "select *\n" +
                            "from tt;", //
                                output);
        }

        {
            String output = SQLUtils.toMySqlString(stmt, new SQLUtils.FormatOption(true, true, true));
            assertEquals("INSERT INTO table1\n" +
                            "WITH tt AS (\n" +
                            "\t\tSELECT *\n" +
                            "\t\tFROM t\n" +
                            "\t)\n" +
                            "SELECT *\n" +
                            "FROM tt;", //
                    output);
        }
    }

    public void test_1() throws Exception {
        String sql = "with tt as (select * from t)  insert into table1 select * from tt;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, true);
        SQLStatement stmt = statementList.get(0);

        {
            String output = SQLUtils.toMySqlString(stmt);
            assertEquals("WITH tt AS (\n" +
                            "\t\tSELECT *\n" +
                            "\t\tFROM t\n" +
                            "\t)\n" +
                            "INSERT INTO table1\n" +
                            "SELECT *\n" +
                            "FROM tt;", //
                                output);
        }
    }
}
