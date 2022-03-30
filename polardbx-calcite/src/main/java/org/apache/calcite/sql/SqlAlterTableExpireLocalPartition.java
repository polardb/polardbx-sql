package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.List;

/**
 * see com/alibaba/polardbx/druid/sql/dialect/mysql/parser/MySqlStatementParser.java:7484
 * key words: "EXPIRE"
 *
 * @author guxu
 */
public class SqlAlterTableExpireLocalPartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("EXPIRE LOCAL PARTITION", SqlKind.EXPIRE_LOCAL_PARTITION);

    protected SqlNode parent;

    private final List<SqlIdentifier> partitions;

    public SqlAlterTableExpireLocalPartition(SqlParserPos pos, List<SqlIdentifier> partitions) {
        super(pos);
        this.partitions = partitions;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public SqlNode getParent() {
        return parent;
    }

    public void setParent(SqlNode parent) {
        this.parent = parent;
    }

    public List<SqlIdentifier> getPartitions() {
        return partitions;
    }

    public void addPartition(SqlIdentifier partition) {
        this.partitions.add(partition);
    }

    @Override
    public String toString() {
        return "EXPIRE LOCAL PARTITION";
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        return new SqlString(dialect ,toString());
    }
}
