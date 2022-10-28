package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.ArrayList;

public class SqlAlterTableRepartitionLocalPartition extends SqlAlterTable {

    protected SqlNode parent;

    protected SqlPartitionByRange localPartition;

    public SqlAlterTableRepartitionLocalPartition(SqlIdentifier tableName, SqlPartitionByRange localPartition) {
        super(null, tableName, null, "", null, new ArrayList<>(), SqlParserPos.ZERO);
        this.name = tableName;
        this.localPartition = localPartition;
    }

    public SqlNode getParent() {
        return parent;
    }

    public void setParent(SqlNode parent) {
        this.parent = parent;
    }

    public SqlPartitionByRange getLocalPartition() {
        return this.localPartition;
    }

    public void setLocalPartition(final SqlPartitionByRange localPartition) {
        this.localPartition = localPartition;
    }

    @Override
    public String toString() {
        return "LOCAL PARTITION";
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        return new SqlString(dialect ,toString());
    }
}
