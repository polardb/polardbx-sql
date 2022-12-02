package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlDropJoinGroup;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class DropJoinGroup extends DDL {
    protected DropJoinGroup(RelOptCluster cluster, RelTraitSet traits,
                            SqlDdl ddl, RelDataType rowType) {
        super(cluster, traits, ddl, rowType);
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier("-", SqlParserPos.ZERO));
    }

    public static DropJoinGroup create(SqlDropJoinGroup sqlDropJoinGroup, RelDataType rowType, RelOptCluster cluster) {
        return new DropJoinGroup(cluster, cluster.traitSetOf(Convention.NONE), sqlDropJoinGroup, rowType);
    }

    @Override
    public DropJoinGroup copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new DropJoinGroup(this.getCluster(), traitSet, ((DropJoinGroup) inputs.get(0)).getAst(), rowType);
    }
}
