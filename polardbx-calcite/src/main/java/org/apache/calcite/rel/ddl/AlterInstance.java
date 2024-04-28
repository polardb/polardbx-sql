package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlterInstance;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class AlterInstance extends DDL {
    public AlterInstance(RelOptCluster cluster, RelTraitSet traitSet, SqlNode sqlNode, RelDataType rowType) {
        super(cluster, traitSet, null);
        this.sqlNode = sqlNode;
    }

    public static AlterInstance create(SqlAlterInstance alterInstance, RelDataType rowType, RelOptCluster cluster) {
        return new AlterInstance(cluster, cluster.traitSetOf(Convention.NONE), alterInstance, rowType);
    }

    @Override
    public AlterInstance copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterInstance(this.getCluster(), traitSet, ((AlterInstance) inputs.get(0)).getAst(), rowType);
    }
}
