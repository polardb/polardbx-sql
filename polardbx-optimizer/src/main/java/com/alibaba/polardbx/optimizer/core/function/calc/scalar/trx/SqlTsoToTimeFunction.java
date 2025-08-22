package com.alibaba.polardbx.optimizer.core.function.calc.scalar.trx;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class SqlTsoToTimeFunction extends SqlFunction {
    public SqlTsoToTimeFunction(String funcName) {
        super(
            funcName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000,
            InferTypes.FIRST_KNOWN,
            OperandTypes.ANY_OR_ANY_ANY,
            SqlFunctionCategory.TIMEDATE
        );
    }
}
