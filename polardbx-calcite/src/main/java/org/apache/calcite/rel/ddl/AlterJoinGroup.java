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
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterJoinGroup extends DDL {
    protected AlterJoinGroup(RelOptCluster cluster, RelTraitSet traits,
                             SqlDdl ddl, RelDataType rowType) {
        super(cluster, traits, ddl, rowType);
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier("-", SqlParserPos.ZERO));
    }

    public static AlterJoinGroup create(SqlAlterJoinGroup sqlAlterJoinGroup, RelDataType rowType,
                                        RelOptCluster cluster) {
        return new AlterJoinGroup(cluster, cluster.traitSetOf(Convention.NONE), sqlAlterJoinGroup, rowType);
    }

    @Override
    public AlterJoinGroup copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterJoinGroup(this.getCluster(), traitSet, ((AlterJoinGroup) inputs.get(0)).getAst(), rowType);
    }
}
