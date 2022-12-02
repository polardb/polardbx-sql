package org.apache.calcite.sql;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.Arrays;
import java.util.List;

/**
 * Created by wumu.
 *
 * @author wumu
 */
public class SqlAlterTableRepartition extends SqlAlterTable {
    private String sourceSql;

    private SqlNode sqlPartition = null;
    private SqlIdentifier originTableName;
    private boolean broadcast;
    private boolean single;
    private boolean alignToTableGroup = false;
    private SqlIdentifier tableGroupName;

    private String logicalSecondaryTableName;

    public SqlAlterTableRepartition(SqlIdentifier tableName,
                                    String sql, List<SqlAlterSpecification> alters,
                                    SqlNode sqlPartition,
                                    boolean alignToTableGroup,
                                    SqlIdentifier tableGroupName) {
        super(null, tableName, null, sql, null, alters, SqlParserPos.ZERO);
        this.sourceSql = sql;
        this.sqlPartition = sqlPartition;
        this.originTableName = tableName;
        this.tableGroupName = tableGroupName;
        this.alignToTableGroup = alignToTableGroup;
    }

    static public SqlAlterTableRepartition create(SqlAlterTablePartitionKey sqlAlterTablePartitionKey) {
        SqlAlterTableRepartition sqlAlterPartitionTableRepartition =
            new SqlAlterTableRepartition(sqlAlterTablePartitionKey.getOriginTableName(),
                sqlAlterTablePartitionKey.getSourceSql() ,
                sqlAlterTablePartitionKey.getAlters(), null, false, null);
        sqlAlterPartitionTableRepartition.setBroadcast(sqlAlterTablePartitionKey.isBroadcast());
        sqlAlterPartitionTableRepartition.setSingle(sqlAlterTablePartitionKey.isSingle());
        return sqlAlterPartitionTableRepartition;
    }

    public SqlNode getSqlPartition() {
        return sqlPartition;
    }

    public void setSqlPartition(SqlNode sqlPartition) {
        this.sqlPartition = sqlPartition;
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

    public String getLogicalSecondaryTableName() {
        return logicalSecondaryTableName;
    }

    public void setLogicalSecondaryTableName(String logicalSecondaryTableName) {
        this.logicalSecondaryTableName = logicalSecondaryTableName;
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

    @Override
    public boolean createGsi() {
        return true;
    }

    public boolean isBroadcast() {
        return this.broadcast;
    }

    public void setBroadcast(final boolean broadcast) {
        this.broadcast = broadcast;
    }

    public boolean isSingle() {
        return this.single;
    }

    public void setSingle(final boolean single) {
        this.single = single;
    }

    public boolean isSingleOrBroadcast() {
        return this.single || this.broadcast;
    }

    public boolean isAlignToTableGroup() {
        return alignToTableGroup;
    }

    public void setAlignToTableGroup(boolean alignToTableGroup) {
        this.alignToTableGroup = alignToTableGroup;
    }

    public SqlIdentifier getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(SqlIdentifier tableGroupName) {
        this.tableGroupName = tableGroupName;
    }
}
