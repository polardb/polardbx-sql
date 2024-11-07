package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class AlterTableGroupOptimizePartition extends AlterTableGroupDdl {

    protected AlterTableGroupOptimizePartition(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                               RelDataType rowType,
                                               String tableGroupName) {
        super(cluster, traits, ddl, rowType);
        this.tableGroupName = tableGroupName;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(tableGroupName, SqlParserPos.ZERO));
    }

    public static AlterTableGroupOptimizePartition create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                          RelDataType rowType,
                                                          String tableGroupName) {

        return new AlterTableGroupOptimizePartition(cluster, traits, ddl, rowType, tableGroupName);
    }

    @Override
    public AlterTableGroupOptimizePartition copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableGroupOptimizePartition(this.getCluster(), traitSet, this.ddl, rowType, tableGroupName);
    }

    @Override
    public String getTableGroupName() {
        return tableGroupName;
    }
}
