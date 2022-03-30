package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * @author guxu
 */
public class AlterTableAllocateLocalPartition extends DDL {

    protected AlterTableAllocateLocalPartition(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                               RelDataType rowType) {
        super(cluster, traits, ddl, rowType);
        this.sqlNode = ddl;
    }

    public static AlterTableAllocateLocalPartition create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                          RelDataType rowType) {

        return new AlterTableAllocateLocalPartition(cluster, traits, ddl, rowType);
    }

    @Override
    public AlterTableAllocateLocalPartition copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableAllocateLocalPartition(this.getCluster(), traitSet, this.ddl, rowType);
    }

}
