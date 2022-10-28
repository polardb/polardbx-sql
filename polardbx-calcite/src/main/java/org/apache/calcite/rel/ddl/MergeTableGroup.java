package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlterJoinGroup;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlMergeTableGroup;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MergeTableGroup extends DDL {
    final boolean force;

    protected MergeTableGroup(RelOptCluster cluster, RelTraitSet traits,
                              SqlDdl ddl, RelDataType rowType, boolean force) {
        super(cluster, traits, ddl, rowType);
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier("-", SqlParserPos.ZERO));
        this.force = force;
    }

    public boolean isForce() {
        return force;
    }

    public static MergeTableGroup create(SqlMergeTableGroup sqlMergeTableGroup, RelDataType rowType,
                                         RelOptCluster cluster, boolean force) {
        return new MergeTableGroup(cluster, cluster.traitSetOf(Convention.NONE), sqlMergeTableGroup, rowType, force);
    }

    @Override
    public MergeTableGroup copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new MergeTableGroup(this.getCluster(), traitSet, ((MergeTableGroup) inputs.get(0)).getAst(), rowType, force);
    }
}
