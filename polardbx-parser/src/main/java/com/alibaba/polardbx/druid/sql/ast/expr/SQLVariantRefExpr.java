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
package com.alibaba.polardbx.druid.sql.ast.expr;

import com.alibaba.polardbx.druid.FastsqlException;
import com.alibaba.polardbx.druid.sql.ast.SQLExprImpl;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SQLVariantRefExpr extends SQLExprImpl {

    private String name;

    private boolean global = false;
    private boolean session = false;

    private int index = -1;

    public SQLVariantRefExpr(String name) {
        this.name = name;
    }

    public SQLVariantRefExpr(String name, SQLObject parent) {
        this.name = name;
        this.parent = parent;
    }

    public SQLVariantRefExpr(String name, boolean global) {
        this(name, global, false);
    }

    public SQLVariantRefExpr(String name, boolean global, boolean session) {
        this.name = name;
        this.global = global;
        this.session = session;
    }

    public SQLVariantRefExpr() {

    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void output(Appendable buf) {
        try {
            buf.append(this.name);
        } catch (IOException ex) {
            throw new FastsqlException("output error", ex);
        }
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);

        visitor.endVisit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SQLVariantRefExpr that = (SQLVariantRefExpr) o;
        return global == that.global && session == that.session && index == that.index && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, global, session, index);
    }

    public boolean isGlobal() {
        return global;
    }

    public void setGlobal(boolean global) {
        this.global = global;
    }

    public boolean isSession() {
        return session;
    }

    public void setSession(boolean session) {
        this.session = session;
    }

    public SQLVariantRefExpr clone() {
        SQLVariantRefExpr var = new SQLVariantRefExpr(name, global, session);

        if (attributes != null) {
            var.attributes = new HashMap<String, Object>(attributes.size());
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                String k = entry.getKey();
                Object v = entry.getValue();

                if (v instanceof SQLObject) {
                    var.attributes.put(k, ((SQLObject) v).clone());
                } else {
                    var.attributes.put(k, v);
                }
            }
        }

        var.index = index;
        return var;
    }

    @Override
    public List<SQLObject> getChildren() {
        return Collections.emptyList();
    }
}
