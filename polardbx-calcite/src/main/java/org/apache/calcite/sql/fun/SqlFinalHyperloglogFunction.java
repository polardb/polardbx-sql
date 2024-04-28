package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class SqlFinalHyperloglogFunction extends SqlAggFunction {
    public SqlFinalHyperloglogFunction() {
        super(
            "FINAL_HYPERLOGLOG",
            null,
            SqlKind.FINAL_HYPER_LOGLOG,
            ReturnTypes.BIGINT,
            null,
            OperandTypes.ONE_OR_MORE,
            SqlFunctionCategory.NUMERIC,
            false,
            false);
    }


    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call) {
        return super.deriveType(validator, scope, call);
    }

    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz == SqlSplittableAggFunction.class) {
            return clazz.cast(SqlSplittableAggFunction.CountSplitter.INSTANCE);
        }
        return super.unwrap(clazz);
    }
}

