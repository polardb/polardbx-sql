package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTableDiscardTableSpace;
import org.apache.calcite.sql.SqlAlterTableImportTableSpace;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableImportTableSpace extends DDL {
    protected AlterTableImportTableSpace(RelOptCluster cluster, RelTraitSet traits,
                                         SqlDdl ddl, SqlNode tableNameNode) {
        super(cluster, traits, null);
        this.sqlNode = ddl;
        this.setTableName(tableNameNode);
    }

    public static AlterTableImportTableSpace create(SqlAlterTableImportTableSpace sqlAlterTableImportTableSpace,
                                                    SqlNode tableNameNode, RelOptCluster cluster) {
        return new AlterTableImportTableSpace(cluster, cluster.traitSetOf(Convention.NONE),
            sqlAlterTableImportTableSpace, tableNameNode);
    }

    @Override
    public AlterTableImportTableSpace copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableImportTableSpace(this.getCluster(), traitSet,
            ((AlterTableImportTableSpace) inputs.get(0)).getAst(), getTableName());
    }
}
