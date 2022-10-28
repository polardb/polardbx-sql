package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dylan
 */

public class DropProcedure extends DDL {

    protected DropProcedure(RelOptCluster cluster, RelTraitSet traits, RelNode input, SqlNode sqlNode,
                            SqlNode tableNameNode) {
        super(cluster, traits, input);
        this.sqlNode = sqlNode;
        this.setTableName(tableNameNode);
    }

    public static DropProcedure create(RelOptCluster cluster, RelNode input, SqlNode sqlNode, SqlNode tableName) {
        return new DropProcedure(cluster, cluster.traitSet(), input, sqlNode, tableName);
    }

    @Override
    public DropProcedure copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new DropProcedure(this.getCluster(),
            traitSet,
            null,
            this.sqlNode,
            getTableName());
    }
}



