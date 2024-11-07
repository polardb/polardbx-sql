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
public class AlterTableArchivePartition extends DDL {

    protected AlterTableArchivePartition(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl, SqlNode tableName) {
        super(cluster, traits, null);
        this.sqlNode = ddl;
        this.setTableName(tableName);
    }

    public static AlterTableArchivePartition create(RelOptCluster cluster, SqlDdl ddl, SqlNode tableName) {
        return new AlterTableArchivePartition(cluster, cluster.traitSet(), ddl, tableName);
    }

    @Override
    public AlterTableArchivePartition copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableArchivePartition(this.getCluster(), traitSet, this.ddl, getTableName());
    }
}

