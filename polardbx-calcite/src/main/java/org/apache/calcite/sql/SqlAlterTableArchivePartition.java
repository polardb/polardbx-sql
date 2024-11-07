package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * @author wumu
 */
public class SqlAlterTableArchivePartition extends SqlAlterTable {

    private final SqlIdentifier originTableName;

    private Set<String> targetPartitions;
    private final boolean subPartitionsArchive;

    public SqlAlterTableArchivePartition(SqlIdentifier tableName,
                                         String sql,
                                         Set<String> targetPartitions,
                                         boolean subPartitionsArchive) {
        super(null, tableName, null, sql, null, new ArrayList<>(), SqlParserPos.ZERO);
        originTableName = tableName;
        this.targetPartitions = targetPartitions;
        this.subPartitionsArchive = subPartitionsArchive;
    }

    @Override
    public SqlIdentifier getOriginTableName() {
        return originTableName;
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
        return getSourceSql();
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

    public Set<String> getTargetPartitions() {
        return targetPartitions;
    }

    public void setTargetPartitions(Set<String> targetPartitions) {
        this.targetPartitions = targetPartitions;
    }

    public boolean isSubPartitionsArchive() {
        return subPartitionsArchive;
    }

}
