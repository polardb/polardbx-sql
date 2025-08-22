package org.apache.calcite.sql;

import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class SqlPreFilterFunction extends SqlFunction {
    public SqlPreFilterFunction() {
        super("PRE_FILTER",
            SqlKind.PRE_FILTER,
            ReturnTypes.BOOLEAN_NULLABLE,
            InferTypes.FIRST_KNOWN,
            OperandTypes.ANY,
            SqlFunctionCategory.SYSTEM);
    }

    public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
        writer.keyword("TRUE");
    }
}

