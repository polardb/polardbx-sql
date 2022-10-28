package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * @author wumu
 */
public class AlterTablePartitionCount extends DDL {
    protected AlterTablePartitionCount(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl, SqlNode tableName) {
        super(cluster, traits, null);
        this.sqlNode = ddl;
        this.setTableName(tableName);
    }

    public static AlterTablePartitionCount create(RelOptCluster cluster, SqlDdl ddl, SqlNode tableName) {
        return new AlterTablePartitionCount(cluster, cluster.traitSet(), ddl, tableName);
    }

    @Override
    public AlterTablePartitionCount copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTablePartitionCount(this.getCluster(), traitSet, this.ddl, getTableName());
    }
}
