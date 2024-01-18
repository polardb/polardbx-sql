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

public class SQLCurrentTimeExpr extends SQLExprImpl {
    private final Type type;

    public SQLCurrentTimeExpr(Type type) {
        if (type == null) {
            throw new NullPointerException();
        }

        this.type = type;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        v.visit(this);
        v.endVisit(this);
    }

    public Type getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (getClass() != o.getClass()) {
            return false;
        }

        SQLCurrentTimeExpr that = (SQLCurrentTimeExpr) o;

        return type == that.type;
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    public SQLCurrentTimeExpr clone() {
        return new SQLCurrentTimeExpr(type);
    }

    public static enum Type {
        CURRENT_TIME("CURRENT_TIME"),
        CURRENT_DATE("CURRENT_DATE"),
        CURDATE("CURDATE"),
        CURTIME("CURTIME"),
        CURRENT_TIMESTAMP("CURRENT_TIMESTAMP"),
        LOCALTIME("LOCALTIME"),
        LOCALTIMESTAMP("LOCALTIMESTAMP"),
        SYSDATE("SYSDATE"),
        UTC_DATE("UTC_DATE"),
        UTC_TIME("UTC_TIME"),
        UTC_TIMESTAMP("UTC_TIMESTAMP"),
        ;

        public final String name;
        public final String name_lower;

        Type(String name) {
            this.name = name;
            this.name_lower = name.toLowerCase();
        }
    }
}
