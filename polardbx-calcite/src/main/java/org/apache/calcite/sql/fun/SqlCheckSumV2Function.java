package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author yaozhili
 */
public class SqlCheckSumV2Function extends SqlAggFunction {
    public SqlCheckSumV2Function() {
        super(
            "CHECK_SUM_V2",
            null,
            SqlKind.CHECK_SUM_V2,
            ReturnTypes.BIGINT,
            null,
            OperandTypes.ONE_OR_MORE,
            SqlFunctionCategory.NUMERIC,
            false,
            false);
    }


    @Override
    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call) {
        // Check for CHECK_SUM_V2(*) function.  If it is we don't
        // want to try and derive the "*"
        if (call.isCheckSumV2Star()) {
            return validator.getTypeFactory().createSqlType(
                SqlTypeName.BIGINT);
        }
        return super.deriveType(validator, scope, call);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz == SqlSplittableAggFunction.class) {
            return clazz.cast(SqlSplittableAggFunction.CountSplitter.INSTANCE);
        }
        return super.unwrap(clazz);
    }
}

