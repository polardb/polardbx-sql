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
package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLAlterTableGroupStatement extends SQLStatementImpl implements SQLAlterStatement {
    private SQLName           name;

    private List<SQLAssignItem> options = new ArrayList<SQLAssignItem>();
    private SQLAlterTableGroupItem item;

    public SQLAlterTableGroupStatement() {

    }

    public SQLAlterTableGroupStatement(DbType dbType) {
        this.setDbType(dbType);
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        if (name != null) {
            name.setParent(this);
        }
        this.name = name;
    }

    public void setItem(SQLAlterTableGroupItem item) {
        this.item = item;
    }

    public SQLAlterTableGroupItem getItem() {
        return item;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public List<SQLAssignItem> getOptions() {
        return options;
    }

}
