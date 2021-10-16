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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

/**
 * @author busu
 * date: 2021/5/23 8:40 下午
 */
public class SqlSlowSqlCcl extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlSlowSqlCclOperator();

    private SqlIdentifier operation;

    private List<SqlNode> params;

    public SqlSlowSqlCcl(SqlIdentifier operation, List<SqlNode> params, SqlParserPos pos) {
        super(pos);
        this.operation = operation;
        this.params = params;
    }

    public SqlIdentifier getOperation() {
        return operation;
    }

    public void setOperation(SqlIdentifier operation) {
        this.operation = operation;
    }

    public List<SqlNode> getParams() {
        return params;
    }

    public void setParams(List<SqlNode> params) {
        this.params = params;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.SLOW_SQL_CCL;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("SLOW_SQL_CCL");
        if (operation != null) {
            operation.unparse(writer, leftPrec, rightPrec);
        }
        if (params != null) {
            for (SqlNode sqlNode : params) {
                sqlNode.unparse(writer, leftPrec, rightPrec);
            }
        }
        writer.endList(selectFrame);
    }

    public static class SqlSlowSqlCclOperator extends SqlSpecialOperator {

        public SqlSlowSqlCclOperator() {
            super("SLOW_SQL_CCL", SqlKind.SLOW_SQL_CCL);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Count", 0,
                typeFactory.createSqlType(SqlTypeName.INTEGER_UNSIGNED)));

            return typeFactory.createStructType(columns);
        }
    }
}
