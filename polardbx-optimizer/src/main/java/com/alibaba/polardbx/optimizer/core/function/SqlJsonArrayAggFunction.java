package com.alibaba.polardbx.optimizer.core.function;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;

public class SqlJsonArrayAggFunction extends SqlAggFunction {

    public SqlJsonArrayAggFunction() {
        super("JSON_ARRAYAGG",
            null,
            SqlKind.JSON_ARRAYAGG,
            ReturnTypes.JSON,
            InferTypes.FIRST_KNOWN,
            OperandTypes.family(SqlTypeFamily.ANY),
            SqlFunctionCategory.SYSTEM,
            false,
            false);
    }

}
