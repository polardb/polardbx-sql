package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableMergePartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("MERGE PARTITION", SqlKind.MERGE_PARTITION);
    private final SqlNode targetPartitionName;
    private final List<SqlNode> oldPartitions;

    public SqlAlterTableMergePartition(SqlParserPos pos, SqlNode targetPartitionName, List<SqlNode> oldPartitions) {
        super(pos);
        this.targetPartitionName = targetPartitionName;
        this.oldPartitions = oldPartitions == null ? new ArrayList<>() : oldPartitions;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public SqlNode getTargetPartitionName() {
        return targetPartitionName;
    }

    public List<SqlNode> getOldPartitions() {
        return oldPartitions;
    }
}

