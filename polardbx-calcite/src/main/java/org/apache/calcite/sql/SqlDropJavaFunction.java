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

public class SqlDropJavaFunction extends SqlDdl {

    private static final SqlSpecialOperator OPERATOR = new SqlDropJavaFunctionOperator();

    protected SqlIdentifier funcName;
    private boolean ifExists;

    public SqlDropJavaFunction(SqlParserPos pos, SqlIdentifier funcName, boolean ifExists) {
        super(OPERATOR, pos);
        this.funcName = funcName;
        this.ifExists = ifExists;
    }

    @Override
    public void unparse(SqlWriter writer, int lefPrec, int rightPrec) {
        writer.keyword("DROP JAVA FUNCTION");

        if (ifExists) {
            writer.keyword("IF EXISTS");
        }
        funcName.unparse(writer, lefPrec, rightPrec);
    }

    public SqlIdentifier getFuncName() {
        return funcName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public static class SqlDropJavaFunctionOperator extends SqlSpecialOperator {

        public SqlDropJavaFunctionOperator() {
            super("DROP_JAVA_FUNCTION", SqlKind.DROP_JAVA_FUNCTION);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("DROP_JAVA_FUNCTION_RESULT",
                    0,
                    columnType)));
        }
    }
}
