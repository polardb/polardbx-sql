package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableMovePartition extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("MOVE PARTITION", SqlKind.MOVE_PARTITION);
    private final Map<SqlNode, List<SqlNode>> instPartitions;
    private SqlAlterTableGroup parent;
    private Map<String, Set<String>> targetPartitions;

    public SqlAlterTableMovePartition(SqlParserPos pos, Map<SqlNode, List<SqlNode>> instPartitions, Map<String, Set<String>> targetPartitions) {
        super(pos);
        this.instPartitions = instPartitions;
        this.targetPartitions = targetPartitions;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public SqlAlterTableGroup getParent() {
        return parent;
    }

    public void setParent(SqlAlterTableGroup parent) {
        this.parent = parent;
    }

    public Map<SqlNode, List<SqlNode>> getInstPartitions() {
        return instPartitions;
    }

    public Map<String, Set<String>> getTargetPartitions() {
        return targetPartitions;
    }

    public void setTargetPartitions(Map<String, Set<String>> targetPartitions) {
        this.targetPartitions = targetPartitions;
    }
}