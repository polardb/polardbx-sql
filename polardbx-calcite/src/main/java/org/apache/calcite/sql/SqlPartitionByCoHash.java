package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.Litmus;

/**
 * @author chenghui.lch
 */
public class SqlPartitionByCoHash extends SqlPartitionBy {

    public SqlPartitionByCoHash(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
        if (!super.equalsDeep(node, litmus, context)) {
            return false;
        }
        return true;
    }
}