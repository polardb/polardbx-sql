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
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLImportTableStatement extends SQLStatementImpl {
    private boolean             extenal;
    private SQLExprTableSource  table;
    private List<SQLAssignItem> partition = new ArrayList<SQLAssignItem>();
    private SQLExpr             from;
    private SQLExpr             location;
    private SQLIntegerExpr version;// for ads
    private boolean usingBuild = false;

    public SQLImportTableStatement() {
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

    public SQLExpr getFrom() {
        return from;
    }

    public void setFrom(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.from = x;
    }

    public SQLExpr getLocation() {
        return location;
    }

    public void setLocation(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.location = x;
    }

    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, table);
            acceptChild(v, partition);
            acceptChild(v, from);
            acceptChild(v, location);
            acceptChild(v, version);
        }
        v.endVisit(this);
    }

    public SQLIntegerExpr getVersion() {
        return version;
    }

    public void setVersion(SQLIntegerExpr version) {
        this.version = version;
    }

    public boolean isUsingBuild() {
        return usingBuild;
    }

    public void setUsingBuild(boolean usingBuild) {
        this.usingBuild = usingBuild;
    }

    public boolean isExtenal() {
        return extenal;
    }

    public void setExtenal(boolean extenal) {
        this.extenal = extenal;
    }
}
