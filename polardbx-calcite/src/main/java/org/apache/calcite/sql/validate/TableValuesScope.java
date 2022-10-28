package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlNode;

/**
 * @author lijiu.lzw
 */
public class TableValuesScope extends ListScope {
    private final SqlNode sqlNode;

    public TableValuesScope(SqlValidatorScope parent, SqlNode sqlNode) {
        super(parent);
        this.sqlNode = sqlNode;
    }

    @Override
    public SqlNode getNode() {
        return sqlNode;
    }
}
