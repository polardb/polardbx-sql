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
import org.junit.Assert;

import java.util.Set;

public class SchemaStatTest13 extends TestCase {

    public void test_schemaStat() throws Exception {
        String sql = "select a.id, b.name from (select t1.* from table1 t1 left join table3 t3 on cast(t1.id as bigint) = t3.id) a inner join table2 b on a.id = b.id";

        DbType dbType = JdbcConstants.MYSQL;
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);
        SQLStatement stmt = parser.parseStatementList().get(0);

        System.out.println(stmt);

        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
        stmt.accept(statVisitor);

        Set<TableStat.Relationship> relationships = statVisitor.getRelationships();

        System.out.println("columns : " + statVisitor.getColumns());
        System.out.println("groups : " + statVisitor.getGroupByColumns()); // group by
        System.out.println("relationships : " + statVisitor.getRelationships()); // group by
        System.out.println("conditions : " + statVisitor.getConditions());
        assertEquals(2, relationships.size());

        Assert.assertEquals(5, statVisitor.getColumns().size());
        Assert.assertEquals(4, statVisitor.getConditions().size());
        assertEquals(0, statVisitor.getFunctions().size());
    }
}
