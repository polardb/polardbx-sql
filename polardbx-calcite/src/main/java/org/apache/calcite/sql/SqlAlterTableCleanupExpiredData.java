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
public class SqlAlterTableCleanupExpiredData extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CLEANUP EXPIRED DATA", SqlKind.CLEANUP_EXPIRED_DATA);

    public SqlAlterTableCleanupExpiredData(SqlParserPos pos, List<SqlIdentifier> partitions) {
        super(pos);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }


    @Override
    public String toString() {
        return "CLEANUP EXPIRED DATA";
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        return new SqlString(dialect ,toString());
    }


    @Override
    public boolean supportFileStorage() { return true;}
}
