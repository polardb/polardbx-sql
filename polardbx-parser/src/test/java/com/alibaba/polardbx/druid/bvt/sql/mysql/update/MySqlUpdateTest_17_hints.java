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
package com.alibaba.polardbx.druid.bvt.sql.mysql.update;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.stat.TableStat.Column;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlUpdateTest_17_hints extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "/*TDDL:node('node_name')*/update/*+TDDL:node('node_name')*/table_1 set id = 1 where id < 10;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        SQLStatement stmt = statementList.get(0);
        print(statementList);

        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

//        System.out.println("Tables : " + visitor.getTables());
//        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(1, visitor.getTables().size());
        assertEquals(1, visitor.getColumns().size());
        // assertEquals(2, visitor.getConditions().size());

        assertTrue(visitor.containsTable("table_1"));

        assertTrue(visitor.getColumns().contains(new Column("table_1", "id")));

        {
            String output = SQLUtils.toMySqlString(stmt);
            assertEquals("/*TDDL:node('node_name')*/\n" +
                            "UPDATE /*+TDDL:node('node_name')*/ table_1\n" +
                            "SET id = 1\n" +
                            "WHERE id < 10;", //
                                stmt.toString());
        }
        {
            String output = SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION);
            assertEquals("/*TDDL:node('node_name')*/\n" +
                            "update /*+TDDL:node('node_name')*/ table_1\n" +
                            "set id = 1\n" +
                            "where id < 10;", //
                                stmt.toLowerCaseString());
        }

        //assertTrue(WallUtils.isValidateMySql(sql));

        {
            SQLUpdateStatement update = (SQLUpdateStatement) stmt;
            SQLExpr where = update.getWhere();
            assertEquals("id < 10", where.toString());
        }

    }


}
