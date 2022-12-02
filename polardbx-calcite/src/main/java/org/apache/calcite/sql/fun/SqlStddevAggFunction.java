package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

public class SqlStddevAggFunction extends SqlAggFunction {
    public SqlStddevAggFunction() {
        super(
            "STDDEV",
            null,
            SqlKind.STDDEV,
            ReturnTypes.DOUBLE_NULLABLE,
            null,
            OperandTypes.family(SqlTypeFamily.ANY),
            SqlFunctionCategory.NUMERIC,
            false,
            false);
    }
}
