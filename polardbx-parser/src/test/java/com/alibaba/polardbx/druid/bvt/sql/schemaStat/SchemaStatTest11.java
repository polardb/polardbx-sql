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

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.polardbx.druid.stat.TableStat;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.Set;

public class SchemaStatTest11 extends TestCase {

    public void test_schemaStat() throws Exception {
        String sql = "select a.id, b.name from (select * from table1) a inner join table2 b on a.id = b.id";

        DbType dbType = JdbcConstants.MYSQL;
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        SQLStatement stmt = parser.parseStatementList().get(0);

        System.out.println(stmt);

        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
        stmt.accept(statVisitor);

        Set<TableStat.Relationship> relationships = statVisitor.getRelationships();
        for (TableStat.Relationship relationship : relationships) {
            System.out.println(relationship); // table1.id = table2.id
        }

        System.out.println(statVisitor.getColumns());
        System.out.println(statVisitor.getGroupByColumns()); // group by
        System.out.println("relationships : " + statVisitor.getRelationships()); // group by
        System.out.println(statVisitor.getConditions());
        assertEquals(1, relationships.size());

        assertEquals(4, statVisitor.getColumns().size());
        assertEquals(2, statVisitor.getConditions().size());
        assertEquals(0, statVisitor.getFunctions().size());

        assertTrue(statVisitor.containsColumn("table1", "*"));
        assertTrue(statVisitor.containsColumn("table1", "id"));
        assertTrue(statVisitor.containsColumn("table2", "id"));
        assertTrue(statVisitor.containsColumn("table2", "name"));
    }
}
