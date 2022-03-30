package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableGroupSplitPartitionByHotValue extends DDL {
    final String tableGroupName;
    final Map<SqlNode, RexNode> partBoundExprInfo;

    protected AlterTableGroupSplitPartitionByHotValue(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                      RelDataType rowType,
                                                      Map<SqlNode, RexNode> partBoundExprInfo,
                                                      String tableGroupName) {
        super(cluster, traits, ddl, rowType);
        this.tableGroupName = tableGroupName;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(tableGroupName, SqlParserPos.ZERO));
        this.partBoundExprInfo = partBoundExprInfo;

    }

    public static AlterTableGroupSplitPartitionByHotValue create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                                 RelDataType rowType,
                                                                 Map<SqlNode, RexNode> partBoundExprInfo,
                                                                 String tableGroupName) {

        return new AlterTableGroupSplitPartitionByHotValue(cluster, traits, ddl, rowType, partBoundExprInfo,
            tableGroupName);
    }

    @Override
    public AlterTableGroupSplitPartitionByHotValue copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableGroupSplitPartitionByHotValue(this.getCluster(), traitSet, this.ddl, rowType,
            partBoundExprInfo, tableGroupName);
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public Map<SqlNode, RexNode> getPartBoundExprInfo() {
        return partBoundExprInfo;
    }
}
