package com.alibaba.polardbx.optimizer.core.function.calc.scalar.trx;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class SqlTimeToTsoFunction extends SqlFunction {
    public SqlTimeToTsoFunction(String funcName) {
        super(
            funcName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT_UNSIGNED_NULLABLE,
            InferTypes.FIRST_KNOWN,
            OperandTypes.ANY_OR_ANY_ANY,
            SqlFunctionCategory.NUMERIC
        );
    }
}
