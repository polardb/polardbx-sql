package com.alibaba.polardbx.optimizer.core.function;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

public class SqlJsonObjectGlobalAggFunction extends SqlAggFunction {

    public SqlJsonObjectGlobalAggFunction() {
        super("JSON_OBJECT_GLOBALAGG",
            null,
            SqlKind.JSON_OBJECT_GLOBALAGG,
            ReturnTypes.JSON,
            InferTypes.FIRST_KNOWN,
            OperandTypes.family(SqlTypeFamily.STRING),
            SqlFunctionCategory.SYSTEM,
            false,
            false);
    }

}
