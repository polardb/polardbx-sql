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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.polardbx.druid.stat.TableStat;
import junit.framework.TestCase;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class SemanticCheckTest_0 extends TestCase {

    public void test_for_semantic_check() throws Exception {
        String sql = "create view v0 as select f0, f1, f3, f5 from table1 t1 inner join table2 t2 on t1.id = t2.id";

        Set<String> tables = new LinkedHashSet<String>();
        {
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql);
            SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(DbType.mysql);
            for (SQLStatement stmt : stmtList) {
                stmt.accept(statVisitor);
            }

            for (TableStat.Name name : statVisitor.getTables().keySet()) {
                tables.add(name.getName());

            }
        }

        for (String table : tables) {
            System.out.println(table);
        }

        System.out.println("=======================");

        SchemaRepository repository = new SchemaRepository(DbType.mysql);
        repository.acceptDDL("create table table1 (id bigint, f0 varchar(50), f1 varchar(50))");
        repository.acceptDDL("create table table2 (id bigint, f3 varchar(50), f4 varchar(50))");

        {
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, DbType.mysql);

            for (SQLStatement stmt : stmtList) {
                repository.resolve(stmt);
            }

            SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(DbType.mysql);
            for (SQLStatement stmt : stmtList) {
                stmt.accept(statVisitor);
            }

            for (TableStat.Column column : statVisitor.getColumns()) {
                String table = column.getTable();
                System.out.print(table + "." + column.getName());
                if ("UNKNOWN".equals(table)) {
                    System.out.print(" * ");
                }
                System.out.println();
            }
        }
    }


}
