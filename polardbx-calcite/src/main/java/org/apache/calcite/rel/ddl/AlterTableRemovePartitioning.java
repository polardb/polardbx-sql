package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public class AlterTableRemovePartitioning extends DDL {
    protected AlterTableRemovePartitioning(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl, SqlNode tableName) {
        super(cluster, traits, null);
        this.sqlNode = ddl;
        this.setTableName(tableName);
    }

    public static AlterTableRemovePartitioning create(RelOptCluster cluster, SqlDdl ddl, SqlNode tableName) {
        return new AlterTableRemovePartitioning(cluster, cluster.traitSet(), ddl, tableName);
    }

    @Override
    public AlterTableRemovePartitioning copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableRemovePartitioning(this.getCluster(), traitSet, this.ddl, getTableName());
    }
}
