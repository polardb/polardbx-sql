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

package com.alibaba.polardbx.druid.bvt.sql.schemaStat;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

public class SchemaStatTest19 extends TestCase {

    public void test_schemaStat() throws Exception {
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);

        String sql = "select * from table1 a left outer join table2 b on a.id=b.id";

        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, JdbcConstants.MYSQL);
        SQLStatement stmt = parser.parseStatementList().get(0);

        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(repository);
        stmt.accept(statVisitor);

        System.out.println("Tables : " + statVisitor.getTables());
        System.out.println("columns : " + statVisitor.getColumns());
//        System.out.println(statVisitor.getGroupByColumns()); // group by
        System.out.println("relationships : " + statVisitor.getRelationships()); // group by
        System.out.println(statVisitor.getConditions());

        assertEquals(4, statVisitor.getColumns().size());
        assertEquals(2, statVisitor.getConditions().size());
        assertEquals(0, statVisitor.getFunctions().size());

        assertTrue(statVisitor.containsTable("table1"));
        assertTrue(statVisitor.containsColumn("table1", "*"));
        assertTrue(statVisitor.containsColumn("table1", "id"));
        assertTrue(statVisitor.containsColumn("table2", "id"));
        assertTrue(statVisitor.containsColumn("table2", "*"));
    }
}
