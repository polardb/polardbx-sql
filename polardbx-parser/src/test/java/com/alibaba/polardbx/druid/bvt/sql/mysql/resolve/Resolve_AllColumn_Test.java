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

package com.alibaba.polardbx.druid.bvt.sql.mysql.resolve;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.sql.repository.SchemaResolveVisitor;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitorAdapter;
import junit.framework.TestCase;

import java.util.List;

public class Resolve_AllColumn_Test extends TestCase {
    public void test_resolve() throws Exception {
        SchemaRepository repository = new SchemaRepository(DbType.mysql);

        repository.acceptDDL("create table t_emp(emp_id bigint, name varchar(20));");

        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("select * from t_emp");
        repository.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);

        assertEquals("SELECT emp_id, name\n" +
                "FROM t_emp", stmt.toString());

        SQLSelectQueryBlock queryBlock = ((SQLSelectStatement) stmt).getSelect().getQueryBlock();
        SQLIdentifierExpr expr = (SQLIdentifierExpr) queryBlock.getSelectList().get(0).getExpr();
        assertNotNull(expr.getResolvedColumn());

        new SQLASTVisitorAdapter() {
          public boolean visit(SQLSelectQueryBlock queryBlock) {
              final List<SQLSelectItem> selectList = queryBlock.getSelectList();
              for (int i = 0; i < selectList.size(); i++) {
                  final SQLSelectItem selectItem = selectList.get(i);
                  final SQLExpr expr = selectItem.getExpr();
                  if (expr instanceof SQLAllColumnExpr) {

                  } else if (expr instanceof SQLPropertyExpr && ((SQLPropertyExpr) expr).getName().equals("*")) {

                  }

              }
              return true;
          }
        };
    }

    public void test_resolve_1() throws Exception {
        SchemaRepository repository = new SchemaRepository(DbType.mysql);

        repository.acceptDDL("create table t_emp(emp_id bigint, name varchar(20));");


        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("select * from (select * from t_emp) x");
        repository.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);

        assertEquals("SELECT emp_id, name\n" +
                "FROM (\n" +
                "\tSELECT emp_id, name\n" +
                "\tFROM t_emp\n" +
                ") x", stmt.toString());
    }

    public void test_resolve_2() throws Exception {
        SchemaRepository repository = new SchemaRepository(DbType.mysql);

        repository.acceptDDL("create table t_emp(emp_id bigint, name varchar(20));");


        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement("select * from (select * from t_emp union all select * from t_emp) x");
        repository.resolve(stmt, SchemaResolveVisitor.Option.ResolveAllColumn);

        assertEquals("SELECT emp_id, name\n" +
                "FROM (\n" +
                "\tSELECT emp_id, name\n" +
                "\tFROM t_emp\n" +
                "\tUNION ALL\n" +
                "\tSELECT emp_id, name\n" +
                "\tFROM t_emp\n" +
                ") x", stmt.toString());
    }
}
