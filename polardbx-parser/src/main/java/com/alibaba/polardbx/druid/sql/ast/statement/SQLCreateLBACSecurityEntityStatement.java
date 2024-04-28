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

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author pangzhaoxing
 */
public class SQLCreateLBACSecurityEntityStatement extends SQLStatementImpl implements SQLCreateStatement {

    private SQLName entityType;

    private SQLName entityKey;

    private SQLName entityAttr;

    public SQLName getEntityType() {
        return entityType;
    }

    public void setEntityType(SQLName entityType) {
        this.entityType = entityType;
    }

    public SQLName getEntityKey() {
        return entityKey;
    }

    public void setEntityKey(SQLName entityKey) {
        this.entityKey = entityKey;
    }

    public SQLName getEntityAttr() {
        return entityAttr;
    }

    public void setEntityAttr(SQLName entityAttr) {
        this.entityAttr = entityAttr;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            if (null != entityType) {
                entityType.accept(v);
            }
            if (null != entityKey) {
                entityKey.accept(v);
            }
            if (null != entityAttr) {
                entityAttr.accept(v);
            }
        }
    }
}
