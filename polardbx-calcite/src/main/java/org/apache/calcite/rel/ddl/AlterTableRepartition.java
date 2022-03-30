package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;

/**
 * @author wumu
 */
public class AlterTableRepartition extends DDL {
    protected Map<SqlNode, RexNode> allRexExprInfo;

    protected AlterTableRepartition(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl, SqlNode tableName,
                                    Map<SqlNode, RexNode> allRexExprInfo) {
        super(cluster, traits, null);
        this.sqlNode = ddl;
        this.allRexExprInfo = allRexExprInfo;
        this.setTableName(tableName);
    }

    public static AlterTableRepartition create(RelOptCluster cluster, SqlDdl ddl, SqlNode tableName,
                                               Map<SqlNode, RexNode> allRexExprInfo) {
        return new AlterTableRepartition(cluster, cluster.traitSet(), ddl, tableName, allRexExprInfo);
    }

    @Override
    public AlterTableRepartition copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableRepartition(this.getCluster(), traitSet, this.ddl, getTableName(), allRexExprInfo);
    }

    public Map<SqlNode, RexNode> getAllRexExprInfo() {
        return allRexExprInfo;
    }
}
