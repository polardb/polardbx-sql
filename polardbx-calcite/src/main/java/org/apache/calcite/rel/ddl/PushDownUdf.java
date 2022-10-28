package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public class PushDownUdf extends DDL {
    protected PushDownUdf(RelOptCluster cluster, RelTraitSet traits, RelNode input, SqlNode sqlNode,
                          SqlNode tableNameNode) {
        super(cluster, traits, input);
        this.sqlNode = sqlNode;
        this.setTableName(tableNameNode);
    }

    public static PushDownUdf create(RelOptCluster cluster, RelNode input, SqlNode sqlNode, SqlNode tableName) {
        return new PushDownUdf(cluster, cluster.traitSet(), input, sqlNode, tableName);
    }

    @Override
    public PushDownUdf copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new PushDownUdf(this.getCluster(),
            traitSet,
            inputs.get(0),
            this.sqlNode,
            getTableName());
    }
}
