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

package com.alibaba.polardbx.druid.bvt.sql;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.List;

public class SQLSelectQueryBlockTest extends TestCase {
    private final DbType dbType = JdbcConstants.MYSQL;
    private SchemaRepository repository;

    protected void setUp() throws Exception {
        repository = new SchemaRepository(dbType);
    }

    public void test_findTableSource() throws Exception {

        repository.console("create table t_emp(emp_id bigint, name varchar(20));");
        repository.console("create table t_org(org_id bigint, name varchar(20));");

        String sql = "SELECT emp_id, a.name AS emp_name, org_id, b.name AS org_name\n" +
                "FROM t_emp a\n" +
                "\tINNER JOIN t_org b ON a.emp_id = b.org_id";

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        assertEquals(1, stmtList.size());

        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        SQLSelectQueryBlock queryBlock = stmt.getSelect().getQueryBlock();

        // 大小写不敏感
        assertNotNull(queryBlock.findTableSource("A"));
        assertSame(queryBlock.findTableSource("a"), queryBlock.findTableSource("A"));

        assertNull(queryBlock.findTableSourceWithColumn("emp_id"));

        // 使用repository做column resolve
        repository.resolve(stmt);

        assertNotNull(queryBlock.findTableSourceWithColumn("emp_id"));

        SQLExprTableSource tableSource = (SQLExprTableSource) queryBlock.findTableSourceWithColumn("emp_id");
        assertNotNull(tableSource.getSchemaObject());

        SQLCreateTableStatement createTableStmt = (SQLCreateTableStatement) tableSource.getSchemaObject().getStatement();
        assertNotNull(createTableStmt);

        SQLSelectItem selectItem = queryBlock.findSelectItem("org_name");
        assertNotNull(selectItem);
        SQLPropertyExpr selectItemExpr = (SQLPropertyExpr) selectItem.getExpr();
        SQLColumnDefinition column = selectItemExpr.getResolvedColumn();
        assertNotNull(column);
        assertEquals("name", column.getName().toString());
        assertEquals("t_org", (((SQLCreateTableStatement)column.getParent()).getName().toString()));

        assertSame(queryBlock.findTableSource("B"), selectItemExpr.getResolvedTableSource());
    }
}
