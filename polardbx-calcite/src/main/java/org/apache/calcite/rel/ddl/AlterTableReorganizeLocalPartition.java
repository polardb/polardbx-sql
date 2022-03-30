package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import java.util.List;

public class AlterTableReorganizeLocalPartition extends DDL {

    protected AlterTableReorganizeLocalPartition(RelOptCluster cluster,
                                                 RelTraitSet traits,
                                                 SqlDdl ddl,
                                                 RelDataType rowType) {
        super(cluster, traits, ddl, rowType);
        this.sqlNode = ddl;
    }

    public static AlterTableReorganizeLocalPartition create(RelOptCluster cluster,
                                                            RelTraitSet traits,
                                                            SqlDdl ddl,
                                                            RelDataType rowType) {
        return new AlterTableReorganizeLocalPartition(cluster, traits, ddl, rowType);
    }

    @Override
    public AlterTableReorganizeLocalPartition copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableReorganizeLocalPartition(this.getCluster(), traitSet, this.ddl, rowType);
    }

}
