package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class SqlCheckSumMergeFunction extends SqlAggFunction {
    public SqlCheckSumMergeFunction() {
        super(
            "CHECK_SUM_MERGE",
            null,
            SqlKind.CHECK_SUM_MERGE,
            ReturnTypes.BIGINT,
            null,
            OperandTypes.ONE_OR_MORE,
            SqlFunctionCategory.NUMERIC,
            false,
            false);
    }

    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz == SqlSplittableAggFunction.class) {
            return clazz.cast(SqlSplittableAggFunction.CountSplitter.INSTANCE);
        }
        return super.unwrap(clazz);
    }
}

