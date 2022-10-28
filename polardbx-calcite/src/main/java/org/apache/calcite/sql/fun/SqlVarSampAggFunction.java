package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

public class SqlVarSampAggFunction extends SqlAggFunction {
    public SqlVarSampAggFunction() {
        super(
            "VAR_SAMP",
            null,
            SqlKind.VAR_SAMP,
            ReturnTypes.DOUBLE_NULLABLE,
            null,
            OperandTypes.family(SqlTypeFamily.ANY),
            SqlFunctionCategory.NUMERIC,
            false,
            false);
    }
}
