package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDropJavaFunction;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public class DropJavaFunction extends DDL {
    public DropJavaFunction(RelOptCluster cluster, RelTraitSet traitSet, SqlNode sqlNode, RelDataType rowType) {
        super(cluster, traitSet, null);
        this.sqlNode = sqlNode;
        this.rowType = rowType;
    }

    public static DropJavaFunction create(SqlDropJavaFunction DropJavaFunction, RelDataType rowType,
                                          RelOptCluster cluster) {
        return new DropJavaFunction(cluster, cluster.traitSetOf(Convention.NONE), DropJavaFunction, rowType);
    }

    @Override
    public DropJavaFunction copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new DropJavaFunction(this.getCluster(), traitSet, ((DropJavaFunction) inputs.get(0)).getAst(), rowType);
    }
}
