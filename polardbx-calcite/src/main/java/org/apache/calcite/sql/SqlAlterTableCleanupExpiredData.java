package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.List;

public class SqlAlterTableCleanupExpiredData extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CLEANUP EXPIRED DATA", SqlKind.CLEANUP_EXPIRED_DATA);

    protected SqlNode ttlCleanup;
    protected SqlNode ttlCleanupBound;
    protected SqlNode archiveTablePreAllocateCount;
    protected SqlNode archiveTablePostAllocateCount;
    protected SqlNode ttlPartInterval;

    /**
     * <pre>
     * The policy for cleanup data, the default value is
     *  1. "deleting/rebuild_table" for ARCHIVE_TYPE of ROW
     *  2. "drop_partitions" for ARCHIVE_TYPE of PARTITION/SUBPARTITION
     * </pre>
     */
    protected SqlNode ttlCleanupPolicy;

    public SqlAlterTableCleanupExpiredData(SqlParserPos pos) {
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
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public String toString() {
        String str = "CLEANUP EXPIRED DATA";

//        if (ttlCleanup != null) {
//            String.format(" ttl_cleanup='%s'", ttlCleanup.toString());
//        }
//
//        if (ttlCleanup != null) {
//            String.format(" ttl_cleanup='%s'", ttlCleanup.toString());
//        }
//
//        if (ttlCleanup != null) {
//            String.format(" ttl_cleanup='%s'", ttlCleanup.toString());
//        }
//
//        if (ttlCleanup != null) {
//            String.format(" ttl_cleanup='%s'", ttlCleanup.toString());
//        }
//
//        if (ttlCleanup != null) {
//            String.format(" ttl_cleanup='%s'", ttlCleanup.toString());
//        }
        return str;
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        return new SqlString(dialect ,toString());
    }


    @Override
    public boolean supportFileStorage() { return true;}

    public SqlNode getTtlCleanup() {
        return ttlCleanup;
    }

    public void setTtlCleanup(SqlNode ttlCleanup) {
        this.ttlCleanup = ttlCleanup;
    }

    public SqlNode getTtlCleanupBound() {
        return ttlCleanupBound;
    }

    public void setTtlCleanupBound(SqlNode ttlCleanupBound) {
        this.ttlCleanupBound = ttlCleanupBound;
    }

    public SqlNode getArchiveTablePreAllocateCount() {
        return archiveTablePreAllocateCount;
    }

    public void setArchiveTablePreAllocateCount(SqlNode archiveTablePreAllocateCount) {
        this.archiveTablePreAllocateCount = archiveTablePreAllocateCount;
    }

    public SqlNode getArchiveTablePostAllocateCount() {
        return archiveTablePostAllocateCount;
    }

    public void setArchiveTablePostAllocateCount(SqlNode archiveTablePostAllocateCount) {
        this.archiveTablePostAllocateCount = archiveTablePostAllocateCount;
    }

    public SqlNode getTtlPartInterval() {
        return ttlPartInterval;
    }

    public void setTtlPartInterval(SqlNode ttlPartInterval) {
        this.ttlPartInterval = ttlPartInterval;
    }

    public SqlNode getTtlCleanupPolicy() {
        return ttlCleanupPolicy;
    }

    public void setTtlCleanupPolicy(SqlNode ttlCleanupPolicy) {
        this.ttlCleanupPolicy = ttlCleanupPolicy;
    }
}
