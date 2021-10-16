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
package com.alibaba.polardbx.druid.bvt.sql.mysql.drop;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.stat.TableStat;

import java.util.List;

public class MySqlTruncateTest_2 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "/* +TDDL: SCAN()*/ TRUNCATE TABLE drds_shard5_00;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, DbType.mysql, SQLParserFeature.TDDLHint);
        SQLStatement stmt = statementList.get(0);
//        print(statementList);

        assertEquals(1, statementList.size());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        System.out.println("Tables : " + visitor.getTables());
//        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());
        
        assertEquals(1, visitor.getTables().size());
        assertEquals(0, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());

        assertTrue(visitor.getTables().containsKey(new TableStat.Name("drds_shard5_00")));

        assertEquals("/*+TDDL: SCAN()*/\n" +
                "TRUNCATE TABLE drds_shard5_00;", stmt.toString());
//        assertTrue(visitor.getColumns().contains(new Column("mytable", "last_name")));
    }
}
