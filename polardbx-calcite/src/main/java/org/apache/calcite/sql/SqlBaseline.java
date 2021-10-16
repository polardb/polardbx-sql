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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @version 1.0
 * @ClassName SqlBaseline
 * @description
 * @Author zzy
 * @Date 2019/9/18 10:43
 */
public class SqlBaseline extends SqlDal {

    private final SqlSpecialOperator operator;

    private String operation;
    private List<Long> baselineIds = new ArrayList<>();

    private SqlSelect select;
    private String sql;
    private String parameterizedSql;
    private String hint;

    public SqlBaseline(SqlParserPos pos, String operation, List<Long> baselineIds, SqlSelect select) {
        super(pos);
        this.operands = new ArrayList<>(0);
        this.operation = operation;
        this.baselineIds.addAll(baselineIds);
        this.select = select;
        this.operator = new SqlBaselineOperator(operation);
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public List<Long> getBaselineIds() {
        return baselineIds;
    }

    public void setBaselineIds(List<Long> baselineIds) {
        this.baselineIds.clear();
        this.baselineIds.addAll(baselineIds);
    }

    public SqlSelect getSelect() {
        return select;
    }

    public void setSelect(SqlSelect select) {
        this.select = select;
    }

    public String getHint() {
        return hint;
    }

    public void setHint(String hint) {
        this.hint = hint;
    }


    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getParameterizedSql() {
        return parameterizedSql;
    }

    public void setParameterizedSql(String parameterizedSql) {
        this.parameterizedSql = parameterizedSql;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("BASELINE");
        writer.sep(operation);
        if (baselineIds.size() > 0) {
            writer.print(String.valueOf(baselineIds.get(0)));
            for (int i = 1; i < baselineIds.size(); i++) {
                writer.print(",");
                writer.print(String.valueOf(baselineIds.get(i)));
            }
        } else if (operation.equalsIgnoreCase("ADD") && select != null) {
            select.unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.BASELINE;
    }

    public static class SqlBaselineOperator extends SqlSpecialOperator {

        private String baselineOp;

        public SqlBaselineOperator(String baselineOp) {
            super("BASELINE", SqlKind.BASELINE);
            this.baselineOp = baselineOp;
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            if (baselineOp.equalsIgnoreCase("LIST")) {
                columns.add(new RelDataTypeFieldImpl("BASELINE_ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
                columns.add(new RelDataTypeFieldImpl("PARAMETERIZED_SQL", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("PLAN_ID", 2, typeFactory.createSqlType(SqlTypeName.INTEGER)));
                columns.add(new RelDataTypeFieldImpl("EXTERNALIZED_TYPE", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("FIXED", 4, typeFactory.createSqlType(SqlTypeName.TINYINT)));
                columns.add(new RelDataTypeFieldImpl("ACCEPTED", 5, typeFactory.createSqlType(SqlTypeName.TINYINT)));
            } else {
                columns.add(new RelDataTypeFieldImpl("BASELINE_ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
                columns.add(new RelDataTypeFieldImpl("STATUS", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            }

            return typeFactory.createStructType(columns);
        }
    }

}
