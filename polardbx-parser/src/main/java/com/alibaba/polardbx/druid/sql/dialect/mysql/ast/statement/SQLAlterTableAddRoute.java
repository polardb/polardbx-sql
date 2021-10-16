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
package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLAlterTableAddRoute extends SQLObjectImpl implements SQLAlterTableItem {
    private SQLCharExpr scatteredId;
    private SQLIntegerExpr shardAssignCount;

    private SQLIntegerExpr effectiveAfterSeconds;

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, scatteredId);
            acceptChild(visitor, shardAssignCount);
            acceptChild(visitor, effectiveAfterSeconds);
        }
        visitor.endVisit(this);
    }

    public SQLCharExpr getScatteredId() {
        return scatteredId;
    }

    public void setScatteredId(SQLCharExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.scatteredId = x;
    }

    public SQLIntegerExpr getShardAssignCount() {
        return shardAssignCount;
    }

    public void setShardAssignCount(SQLIntegerExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.shardAssignCount = x;
    }

    public SQLIntegerExpr getEffectiveAfterSeconds() {
        return effectiveAfterSeconds;
    }

    public void setEffectiveAfterSeconds(SQLIntegerExpr x) {
        if (x != null) {
            x.setParent(this);
        }
        this.effectiveAfterSeconds = x;
    }


}
