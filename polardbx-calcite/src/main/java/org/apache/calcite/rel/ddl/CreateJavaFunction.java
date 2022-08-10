package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCreateJavaFunction;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public class CreateJavaFunction extends DDL {
  public CreateJavaFunction(RelOptCluster cluster, RelTraitSet traitSet, SqlNode sqlNode, RelDataType rowType) {
    super(cluster, traitSet, null);
    this.sqlNode = sqlNode;
    this.rowType = rowType;
  }

  public static CreateJavaFunction create(SqlCreateJavaFunction createJavaFunction, RelDataType rowType, RelOptCluster cluster) {
    return new CreateJavaFunction(cluster, cluster.traitSetOf(Convention.NONE), createJavaFunction, rowType);
  }

  @Override
  public CreateJavaFunction copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new CreateJavaFunction(this.getCluster(), traitSet, ((CreateJavaFunction) inputs.get(0)).getAst(), rowType);
  }
}
