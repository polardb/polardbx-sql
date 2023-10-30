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

package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class CreateFileStorageStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private boolean ifNotExists;

    private SQLName engineName;
    private List<SQLAssignItem> withValue;

    public CreateFileStorageStatement() {
        this.withValue = new ArrayList<SQLAssignItem>();
        this.ifNotExists = false;
    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (this.engineName != null) {
                this.engineName.accept(visitor);
            }
            if (this.withValue != null) {
                for (SQLAssignItem item : withValue) {
                    item.accept(visitor);
                }
            }
        }
        visitor.endVisit(this);
    }

    @Override
    public CreateFileStorageStatement clone() {
        CreateFileStorageStatement x = new CreateFileStorageStatement();
        if (this.engineName != null) {
            x.engineName = this.engineName.clone();
        }
        if (this.withValue != null) {
            for (SQLAssignItem item : withValue) {
                x.withValue.add(item.clone());
            }
        }
        x.setIfNotExists(this.isIfNotExists());
        return x;
    }

    public SQLName getEngineName() {
        return engineName;
    }

    public CreateFileStorageStatement setEngineName(SQLName engineName) {
        this.engineName = engineName;
        return this;
    }

    public List<SQLAssignItem> getWithValue() {
        return withValue;
    }

    public boolean isIfNotExists() {
        return this.ifNotExists;
    }

    public void setIfNotExists(final boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }
}
