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

package com.alibaba.polardbx.druid.sql.ast;

import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLDataTypeRefExpr extends SQLExprImpl {
    private SQLDataType dataType;

    public SQLDataTypeRefExpr(SQLDataType dataType) {
        this.dataType = dataType;
    }

    public SQLDataType getDataType() {
        return dataType;
    }

    public void setDataType(SQLDataType x) {
        if (x != null) {
            x.setParent(this);
        }
        this.dataType = x;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SQLDataTypeRefExpr that = (SQLDataTypeRefExpr) o;

        return dataType != null ? dataType.equals(that.dataType) : that.dataType == null;
    }

    @Override
    public int hashCode() {
        return dataType != null ? dataType.hashCode() : 0;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, dataType);
        }
        v.endVisit(this);
    }

    @Override
    public SQLExpr clone() {
        return new SQLDataTypeRefExpr(
            dataType == null
                ? null
                : dataType.clone());
    }
}
