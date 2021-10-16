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

import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLTableLike extends SQLObjectImpl implements SQLTableElement {
    private SQLExprTableSource table;
    private boolean includeProperties = false;
    private boolean excludeProperties = false;

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, table);
        }
        v.endVisit(this);
    }

    public SQLTableLike clone() {
        SQLTableLike x = new SQLTableLike();
        if (table != null) {
            x.setTable(table.clone());
        }

        return x;
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

    public boolean isIncludeProperties() {
        return includeProperties;
    }

    public void setIncludeProperties(boolean includeProperties) {
        this.includeProperties = includeProperties;
    }

    public boolean isExcludeProperties() {
        return excludeProperties;
    }

    public void setExcludeProperties(boolean excludeProperties) {
        this.excludeProperties = excludeProperties;
    }
}
