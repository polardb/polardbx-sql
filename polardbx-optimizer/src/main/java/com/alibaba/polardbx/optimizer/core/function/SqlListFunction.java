package com.alibaba.polardbx.optimizer.core.function;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class SqlListFunction extends SqlFunction {
    public static String name = "list";

    public SqlListFunction() {
        super(
            name,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.RETURN_INPUT_TYPE,
            InferTypes.FIRST_KNOWN,
            OperandTypes.ANY,
            SqlFunctionCategory.STRING
        );
    }
}
