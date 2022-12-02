package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

public class SqlPushDownUdf extends SqlDdl{
    private static final SqlSpecialOperator OPERATOR = new SqlPushDownUdfOperator();

    public SqlPushDownUdf(SqlParserPos pos) {
        super(OPERATOR, pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("pushdown udf");
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.PUSH_DOWN_UDF;
    }

    public static class SqlPushDownUdfOperator extends SqlSpecialOperator {

        public SqlPushDownUdfOperator() {
            super("PushDownUdf", SqlKind.PUSH_DOWN_UDF);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("PushDownUdf",
                    0,
                    columnType)));
        }
    }
}
