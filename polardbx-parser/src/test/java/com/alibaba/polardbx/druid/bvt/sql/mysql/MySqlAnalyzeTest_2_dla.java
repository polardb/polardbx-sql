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
package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;

import java.util.List;

public class MySqlAnalyzeTest_2_dla extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS;";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);
//        print(statementList);

        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);
        
        String output = SQLUtils.toMySqlString(stmt);
        assertEquals("ANALYZE TABLE Table1 PARTITION (ds = '2008-04-09', hr = 11) COMPUTE STATISTICS;", //
                            output);

//        System.out.println("Tables : " + visitor.getTables());
//        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());
        
        assertEquals(1, visitor.getTables().size());
        assertEquals(0, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());

//        assertTrue(visitor.getTables().containsKey(new TableStat.Name("City")));
//        assertTrue(visitor.getTables().containsKey(new TableStat.Name("t2")));

//        assertTrue(visitor.getColumns().contains(new Column("t2", "id")));
    }

    public void test_1() throws Exception {
        String sql = "ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS FOR COLUMNS;";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);
//        print(statementList);

        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        String output = SQLUtils.toMySqlString(stmt);
        assertEquals("ANALYZE TABLE Table1 PARTITION (ds = '2008-04-09', hr = 11) COMPUTE STATISTICS FOR COLUMNS;", //
                output);

//        System.out.println("Tables : " + visitor.getTables());
//        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(1, visitor.getTables().size());
        assertEquals(0, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());

//        assertTrue(visitor.getTables().containsKey(new TableStat.Name("City")));
//        assertTrue(visitor.getTables().containsKey(new TableStat.Name("t2")));

//        assertTrue(visitor.getColumns().contains(new Column("t2", "id")));
    }

    public void test_2() throws Exception {
        String sql = "ANALYZE TABLE Table1 PARTITION(ds, hr) COMPUTE STATISTICS FOR COLUMNS;";

        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);
//        print(statementList);

        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        assertEquals("ANALYZE TABLE Table1 PARTITION (ds, hr) COMPUTE STATISTICS FOR COLUMNS;", //
                stmt.toString());
        assertEquals("analyze table Table1 partition (ds, hr) compute statistics for columns;", //
                stmt.toLowerCaseString());

//        System.out.println("Tables : " + visitor.getTables());
//        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(1, visitor.getTables().size());
        assertEquals(0, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());

//        assertTrue(visitor.getTables().containsKey(new TableStat.Name("City")));
//        assertTrue(visitor.getTables().containsKey(new TableStat.Name("t2")));

//        assertTrue(visitor.getColumns().contains(new Column("t2", "id")));
    }
}
