package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlConvertAllSequences;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ConvertAllSequences extends DDL {
    public ConvertAllSequences(RelOptCluster cluster, RelTraitSet traitSet, SqlNode sqlNode, RelDataType relDataType) {
        super(cluster, traitSet, null);
        this.sqlNode = sqlNode;
    }

    public static ConvertAllSequences create(SqlConvertAllSequences sqlConvertAllSequences, RelDataType relDataType,
                                             RelOptCluster cluster) {
        return new ConvertAllSequences(cluster, cluster.traitSetOf(Convention.NONE), sqlConvertAllSequences,
            relDataType);
    }

    @Override
    public ConvertAllSequences copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new ConvertAllSequences(this.getCluster(), traitSet, ((ConvertAllSequences) (inputs.get(0))).getAst(),
            rowType);
    }
}
