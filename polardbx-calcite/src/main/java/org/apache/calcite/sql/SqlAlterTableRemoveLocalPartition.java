package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.ArrayList;

public class SqlAlterTableRemoveLocalPartition extends SqlAlterTable {

    protected SqlNode parent;

    public SqlAlterTableRemoveLocalPartition(SqlIdentifier tableName) {
        super(null, tableName, null, "", null, new ArrayList<>(), SqlParserPos.ZERO);
        this.name = tableName;
    }

    public SqlNode getParent() {
        return parent;
    }

    public void setParent(SqlNode parent) {
        this.parent = parent;
    }

    @Override
    public String toString() {
        return "REMOVE LOCAL PARTITIONING";
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        return new SqlString(dialect ,toString());
    }
}
