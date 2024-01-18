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

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLCopyFromStatement extends SQLStatementImpl {
    private SQLExprTableSource table;
    private final List<SQLName> columns = new ArrayList<SQLName>();
    private SQLExpr from;
    private SQLExpr accessKeyId;
    private SQLExpr accessKeySecret;
    private final List<SQLAssignItem> options = new ArrayList<SQLAssignItem>();
    private final List<SQLAssignItem> partitions = new ArrayList<SQLAssignItem>();

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, table);
            acceptChild(v, columns);
            acceptChild(v, partitions);
            acceptChild(v, from);
            acceptChild(v, options);
        }
        v.endVisit(this);
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

    public List<SQLName> getColumns() {
        return columns;
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

    public SQLExpr getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.accessKeyId = x;
    }

    public SQLExpr getAccessKeySecret() {
        return accessKeySecret;
    }

    public void setAccessKeySecret(SQLExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.accessKeySecret = x;
    }

    public List<SQLAssignItem> getOptions() {
        return options;
    }

    public List<SQLAssignItem> getPartitions() {
        return partitions;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
