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

package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLExportTableStatement extends SQLStatementImpl {
    private SQLExprTableSource  table;
    private List<SQLAssignItem> partition = new ArrayList<SQLAssignItem>();
    private SQLExpr             to;
    private SQLExpr             forReplication;

    public SQLExportTableStatement() {
        dbType = DbType.mysql;
    }

    public SQLExprTableSource getTable() {
        return table;
    }

    public void setTable(SQLExprTableSource x) {
        if (x != null) {
            x.setParent(this);
        }
        this.table = x;
    }

    public List<SQLAssignItem> getPartition() {
        return partition;
    }

    public SQLExpr getTo() {
        return to;
    }

    public void setTo(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.to = x;
    }

    public SQLExpr getForReplication() {
        return forReplication;
    }

    public void setForReplication(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.forReplication = x;
    }

    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, table);
            acceptChild(v, partition);
            acceptChild(v, to);
            acceptChild(v, forReplication);
        }
        v.endVisit(this);
    }
}
