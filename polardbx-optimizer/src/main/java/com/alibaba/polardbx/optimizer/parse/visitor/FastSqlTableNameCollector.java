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

package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.polardbx.common.utils.Pair;

import java.util.HashSet;
import java.util.Set;

public class FastSqlTableNameCollector extends MySqlASTVisitorAdapter {

    final Set<Pair<String, String>> schemaTables = new HashSet<>();

    @Override
    public boolean visit(SQLExprTableSource x) {
        schemaTables.add(Pair.of(SQLUtils.normalizeNoTrim(x.getSchema()), SQLUtils.normalizeNoTrim(x.getTableName())));
        return false;
    }

    @Override
    public boolean visit(MySqlDeleteStatement x) {

        // in fastsql, MySqlDeleteStatement.from is null for sql like delete from t1, so we get tables from MySqlDeleteStatement.tableSource
        // but MySqlDeleteStatement.tableSource is wrong for sql like delete a.*,b.* from t1 a,t2 b, and we get tables from MySqlDeleteStatement.from
        // but  MySqlDeleteStatement.tableSource and  MySqlDeleteStatement.from are wrong for sql like delete from a.*,b.* using t1 a, t2 b
        // tmd...
        if (x.getUsing() != null) {
            x.getUsing().accept(this);
        } else if (x.getFrom() != null) {
            x.getFrom().accept(this);
        } else {
            x.getTableSource().accept(this);
        }

        if (x.getWith() != null) {
            x.getWith().accept(this);
        }

        if (x.getWhere() != null) {
            x.getWhere().accept(this);
        }

        if (x.getOrderBy() != null) {
            x.getOrderBy().accept(this);
        }

        if (x.getLimit() != null) {
            x.getLimit().accept(this);
        }
        return false;
    }

    public Set<Pair<String, String>> getTables() {
        return this.schemaTables;
    }
}
