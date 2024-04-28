package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlterJoinGroup;
import org.apache.calcite.sql.SqlAlterTableDiscardTableSpace;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableDiscardTableSpace extends DDL {
    protected AlterTableDiscardTableSpace(RelOptCluster cluster, RelTraitSet traits,
                                          SqlDdl ddl, SqlNode tableNameNode) {
        super(cluster, traits, null);
        this.sqlNode = ddl;
        this.setTableName(tableNameNode);
    }

    public static AlterTableDiscardTableSpace create(SqlAlterTableDiscardTableSpace sqlAlterTableDiscardTableSpace,
                                                     SqlNode tableNameNode, RelOptCluster cluster) {
        return new AlterTableDiscardTableSpace(cluster, cluster.traitSetOf(Convention.NONE),
            sqlAlterTableDiscardTableSpace, tableNameNode);
    }

    @Override
    public AlterTableDiscardTableSpace copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableDiscardTableSpace(this.getCluster(), traitSet,
            ((AlterTableDiscardTableSpace) inputs.get(0)).getAst(), getTableName());
    }
}
