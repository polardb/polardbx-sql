/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.stat.TableStat;
import com.alibaba.polardbx.druid.stat.TableStat.Column;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlDeleteTest_7 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "/*TDDL:node('node_name')*/delete/*+TDDL:node('node_name')*/ from table_1 where id < 10;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL, SQLParserFeature.TDDLHint);
        SQLStatement stmt = statementList.get(0);
        assertEquals("/*TDDL:node('node_name')*/\n" +
                "DELETE /*+TDDL:node('node_name')*/ FROM table_1\n" +
                "WHERE id < 10;", SQLUtils.toMySqlString(stmt));
        assertEquals("/*TDDL:node('node_name')*/\n" +
                "delete /*+TDDL:node('node_name')*/ from table_1\n" +
                "where id < 10;", SQLUtils.toMySqlString(stmt, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        System.out.println(stmt);

        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(1, visitor.getTables().size());
        assertEquals(1, visitor.getColumns().size());
        assertEquals(1, visitor.getConditions().size());

        assertTrue(visitor.getTables().containsKey(new TableStat.Name("table_1")));

        assertTrue(visitor.getColumns().contains(new Column("table_1", "id")));
    }
}
