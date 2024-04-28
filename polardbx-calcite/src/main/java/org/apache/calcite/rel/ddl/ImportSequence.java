package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlImportSequence;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ImportSequence extends DDL {

    public ImportSequence(RelOptCluster cluster, RelTraitSet traitSet, SqlNode sqlNode, RelDataType rowTyep) {
        super(cluster, traitSet, null);
        this.sqlNode = sqlNode;
    }

    public static ImportSequence create(SqlImportSequence sqlImportSequence, RelDataType rowType,
                                        RelOptCluster cluster) {
        return new ImportSequence(cluster, cluster.traitSetOf(Convention.NONE), sqlImportSequence, rowType);
    }

    @Override
    public ImportSequence copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ImportSequence(this.getCluster(), traitSet, ((ImportSequence) inputs.get(0)).getAst(), rowType);
    }
}
