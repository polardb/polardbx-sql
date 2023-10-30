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

package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;

public class SqlPhysicalPartition extends SqlNode {
    SqlIdentifier name;

    public SqlPhysicalPartition(SqlIdentifier name, SqlParserPos sqlParserPos) {
        super(sqlParserPos);
        this.name = name;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlPhysicalPartition(this.name, pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, " PARTITION(", ")");
        writer.sep(this.name.toString());
        writer.endList(frame);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validate(this.name);
    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return visitor.visit(this.name);
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
        if (this == node) {
            return true;
        }

        if (node == null) {
            return false;
        }

        if (node.getClass() != this.getClass()) {
            return false;
        }

        SqlPhysicalPartition objPartBy = (SqlPhysicalPartition) node;

        if (!equalDeep(this.name, objPartBy.name, litmus, context)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append(" PARTITION(");
        sb.append(name);
        sb.append(")");

        return sb.toString();
    }
}
