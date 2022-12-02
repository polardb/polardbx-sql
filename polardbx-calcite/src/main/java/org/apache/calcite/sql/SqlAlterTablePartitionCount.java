package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SqlAlterTablePartitionCount extends SqlAlterTable {

    private int partitionCount;

    private String sourceSql;

    private final SqlIdentifier originTableName;


    public SqlAlterTablePartitionCount(SqlIdentifier tableName, String sql, List<SqlAlterSpecification> alters) {
        super(null, tableName, null, sql, null, alters, SqlParserPos.ZERO);
        this.sourceSql = sql;
        this.originTableName = tableName;
    }

    @Override
    public SqlIdentifier getOriginTableName() {
        return originTableName;
    }

    @Override
    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        unparse(writer, leftPrec, rightPrec, false);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec, boolean withOriginTableName) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "ALTER TABLE", "");

        name.unparse(writer, leftPrec, rightPrec);

        writer.endList(frame);
    }

    @Override
    public void setTargetTable(SqlIdentifier sqlIdentifier) {
        this.name = sqlIdentifier;
    }

    private String prepare() {
        return sourceSql;
    }

    public String getSchemaName() {
        return originTableName.getComponent(0).getLastName();
    }

    public String getPrimaryTableName() {
        return originTableName.getComponent(1).getLastName();
    }

    @Override
    public SqlNode getTargetTable() {
        return super.getTargetTable();
    }

    @Override
    public String toString() {
        return prepare();
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        String sql = prepare();
        return new SqlString(dialect, sql);
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }
}
