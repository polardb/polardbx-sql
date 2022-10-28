package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableGroupAddTable extends DDL {
    final String tableGroupName;
    final boolean force;

    protected AlterTableGroupAddTable(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                      RelDataType rowType,
                                      String tableGroupName,
                                      boolean force) {
        super(cluster, traits, ddl, rowType);
        this.tableGroupName = tableGroupName;
        this.sqlNode = ddl;
        this.force = force;
        this.setTableName(new SqlIdentifier(tableGroupName, SqlParserPos.ZERO));
    }

    public static AlterTableGroupAddTable create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                 RelDataType rowType,
                                                 String tableGroupName,
                                                 boolean force) {

        return new AlterTableGroupAddTable(cluster, traits, ddl, rowType, tableGroupName, force);
    }

    @Override
    public AlterTableGroupAddTable copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableGroupAddTable(this.getCluster(), traitSet, this.ddl, rowType, tableGroupName, force);
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public boolean isForce() {
        return force;
    }
}
