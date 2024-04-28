package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlImportDatabase;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ImportDatabase extends DDL {
    public ImportDatabase(RelOptCluster cluster, RelTraitSet traitSet, SqlNode sqlNode, RelDataType rowType) {
        super(cluster, traitSet, null);
        this.sqlNode = sqlNode;
    }

    public static ImportDatabase create(SqlImportDatabase sqlImportDatabase, RelDataType rowType, RelOptCluster cluster) {
        return new ImportDatabase(cluster, cluster.traitSetOf(Convention.NONE), sqlImportDatabase, rowType);
    }

    @Override
    public ImportDatabase copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ImportDatabase(this.getCluster(), traitSet, ((ImportDatabase) inputs.get(0)).getAst(), rowType);
    }
}

