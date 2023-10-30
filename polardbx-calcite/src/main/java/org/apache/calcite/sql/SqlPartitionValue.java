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

import com.alibaba.polardbx.common.exception.NotSupportException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlPartitionValue extends SqlNode {
    protected Operator operator;
    protected final List<SqlPartitionValueItem> items = new ArrayList<>();

    public SqlPartitionValue(Operator operator, SqlParserPos sqlParserPos) {
        super(sqlParserPos);
        this.operator = operator;
    }

    public List<SqlPartitionValueItem> getItems() {
        return items;
    }

    public SqlNode getLessThanValue() {
        if (!this.operator.equals(Operator.LessThan)) {
            throw new RuntimeException("sql not less than: " + this.operator);
        }
        return getItems().get(0).getValue();
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        SqlPartitionValue copy = new SqlPartitionValue(this.operator, pos);
        copy.getItems().addAll(this.items);
        return copy;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append("values");
        sb.append(" ");
        sb.append(operator.name());
        sb.append(" ");
        sb.append("(");
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            SqlNode item = items.get(i).getValue();
            sb.append(item.toString());
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {

    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        //Todo can't be dynamic function, for example now()
        for (SqlPartitionValueItem sqlNode : items) {
            if (sqlNode.isMaxValue()) {
                continue;
            }
            sqlNode.getValue().accept(new SqlBasicVisitor<Boolean>() {
                @Override
                public Boolean visit(SqlCall call) {
                    final Boolean visited = super.visit(call);
                    final SqlOperator operator = call.getOperator();
                    if (operator.isDynamicFunction()) {

                        String name = operator.getName();
                        if (!(name.equalsIgnoreCase("unix_timestamp") || name.equalsIgnoreCase("to_seconds")
                            || name.equalsIgnoreCase("to_days") || name.equalsIgnoreCase("month"))) {
                            throw new NotSupportException("Dynamic function in partition clause is");
                        }
                    }
                    return visited;
                }

                @Override
                public Boolean visit(SqlLiteral literal) {
                    return false;
                }

                @Override
                public Boolean visit(SqlIdentifier id) {
                    throw new NotSupportException("use column in partition value clause is");
                }
            });
        }
    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return null;
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
        if (this == node) {
            return true;
        }

        if (this.getClass() != node.getClass()) {
            return false;
        }

        SqlPartitionValue sqlPart = (SqlPartitionValue) node;

        if (operator != sqlPart.operator) {
            return false;
        }

        for (int i = 0; i < items.size(); ++i) {
            if (!items.get(i).equalsDeep(sqlPart.items.get(i), litmus, context)) {
                return false;
            }
        }

        return true;
    }

    public Operator getOperator() {
        return operator;
    }

    public enum Operator {
        LessThan,
        In,
        List
    }
}
