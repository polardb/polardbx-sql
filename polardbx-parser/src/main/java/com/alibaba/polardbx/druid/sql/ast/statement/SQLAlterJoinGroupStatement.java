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

import java.util.List;

public class SQLAlterJoinGroupStatement extends SQLStatementImpl implements SQLAlterStatement {
    private SQLName joinGroupName;

    private boolean isAdd;
    private boolean isRemove;
    private List<SQLName> tableNames;

    public SQLAlterJoinGroupStatement() {
    }

    public SQLAlterJoinGroupStatement(DbType dbType) {
        super(dbType);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public String getJoinGroupName() {
        if (joinGroupName == null) {
            return null;
        }

        if (joinGroupName instanceof SQLName) {
            return joinGroupName.getSimpleName();
        }

        return null;
    }

    public void setJoinGroupName(SQLName joinGroupName) {
        this.joinGroupName = joinGroupName;
    }

    public boolean isAdd() {
        return isAdd;
    }

    public void setAdd(boolean add) {
        isAdd = add;
        isRemove = !add;
    }

    public boolean isRemove() {
        return isRemove;
    }

    public void setRemove(boolean remove) {
        isRemove = remove;
        isAdd = !isRemove;
    }

    public List<SQLName> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<SQLName> tableNames) {
        this.tableNames = tableNames;
    }
}
