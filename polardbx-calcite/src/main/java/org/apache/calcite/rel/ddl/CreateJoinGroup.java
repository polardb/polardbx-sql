package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCreateJoinGroup;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class CreateJoinGroup extends DDL {
    protected CreateJoinGroup(RelOptCluster cluster, RelTraitSet traits,
                              SqlDdl ddl, RelDataType rowType) {
        super(cluster, traits, ddl, rowType);
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier("-", SqlParserPos.ZERO));
    }

    public static CreateJoinGroup create(SqlCreateJoinGroup createJoinGroup, RelDataType rowType,
                                         RelOptCluster cluster) {
        return new CreateJoinGroup(cluster, cluster.traitSetOf(Convention.NONE), createJoinGroup, rowType);
    }

    @Override
    public CreateJoinGroup copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new CreateJoinGroup(this.getCluster(), traitSet, ((CreateJoinGroup) inputs.get(0)).getAst(), rowType);
    }
}
