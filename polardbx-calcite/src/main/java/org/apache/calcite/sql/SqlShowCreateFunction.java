package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

public class SqlShowCreateFunction extends SqlShow {
    private static final SqlSpecialOperator OPERATOR = new SqlShowCreateFunction.SqlShowCreateFunctionOperator();

    private SqlNode functionName;
    public SqlShowCreateFunction(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers,
                                 List<SqlNode> operands, SqlNode functionName) {
        super(pos, specialIdentifiers, operands, null, null, null, null,
            specialIdentifiers.size() + operands.size() - 1);
        this.functionName = functionName;
    }

    public static SqlShowCreateFunction create(SqlParserPos pos, SqlNode functionName) {
        return new SqlShowCreateFunction(pos,
            ImmutableList.of(SqlSpecialIdentifier.CREATE, SqlSpecialIdentifier.FUNCTION),
            ImmutableList.of(functionName), functionName);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_CREATE_FUNCTION;
    }

    public SqlNode getFunctionName() {
        return functionName;
    }

    public static class SqlShowCreateFunctionOperator extends SqlSpecialOperator {

        public SqlShowCreateFunctionOperator() {
            super("SHOW_CREATE_FUNCTION", SqlKind.SHOW_CREATE_FUNCTION);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Function", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(
                new RelDataTypeFieldImpl("Create Function", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
