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

package org.apache.calcite.rex;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Objects;

/**
 * @author dylan
 */
public class RexUserVar extends RexNode {

    // ~ Instance fields
    // --------------------------------------------------------

    private final String name;

    private final RelDataType type;
    private final SqlTypeName typeName;

    // ~ Constructors
    // -----------------------------------------------------------

    RexUserVar(String name, RelDataType type, SqlTypeName typeName) {
        this.name = name;
        this.type = Preconditions.checkNotNull(type);
        this.typeName = Preconditions.checkNotNull(typeName);
        this.digest = toJavaString(name);
    }

    // ~ Methods
    // ----------------------------------------------------------------

    private static String toJavaString(String name) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        printAsJava(name, pw);
        pw.flush();
        return sw.toString();
    }

    private static void printAsJava(String name, PrintWriter pw) {
        assert name != null;
        pw.print("@" + name);
    }

    public String getName() {
        return name;
    }

    public SqlTypeName getTypeName() {
        return typeName;
    }

    @Override
    public RelDataType getType() {
        return type;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.USER_VAR;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RexUserVar that = (RexUserVar) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    @Override
    public <R> R accept(RexVisitor<R> visitor) {
        return visitor.visitUserVar(this);
    }

    @Override
    public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
        return visitor.visitUserVar(this, arg);
    }
}

