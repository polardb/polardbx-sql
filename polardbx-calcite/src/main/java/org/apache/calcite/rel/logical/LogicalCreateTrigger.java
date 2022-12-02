package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;

public class LogicalCreateTrigger extends DDL {

    protected LogicalCreateTrigger(RelOptCluster cluster, RelTraitSet traits, RelNode input, SqlNode sqlNode,
                                   SqlNode tableNameNode, RelDataType rowType){
        super(cluster, traits, input);
        this.sqlNode = sqlNode;
        this.setTableName(tableNameNode);
    }

    public static LogicalCreateTrigger create(RelOptCluster cluster, RelNode input, SqlNode sqlNode, SqlNode tableName, RelDataType rowType) {
        return new LogicalCreateTrigger(cluster, cluster.traitSet(), input, sqlNode, tableName, rowType);
    }

    @Override
    public LogicalCreateTrigger copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new LogicalCreateTrigger(this.getCluster(),
            traitSet,
            inputs.get(0),
            this.sqlNode,
            getTableName(),
            rowType);
    }

    @Override
    public RelNode getInput(int i) {
        List<RelNode> inputs = getInputs();
        return inputs.get(i);
    }

    @Override
    public List<RelNode> getInputs() {
        return new ArrayList<RelNode>();
    }
}

