package org.apache.calcite.sql;

import com.alibaba.polardbx.common.utils.Pair;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableRenamePartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("RENAME PARTITION", SqlKind.RENAME_PARTITION);

    private final List<Pair<String, String>> changePartitionsPair;

    public SqlAlterTableRenamePartition(SqlParserPos pos, List<Pair<String, String>> changePartitionsPair) {
        super(pos);
        this.changePartitionsPair = changePartitionsPair;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public List<Pair<String, String>> getChangePartitionsPair() {
        return changePartitionsPair;
    }
}
