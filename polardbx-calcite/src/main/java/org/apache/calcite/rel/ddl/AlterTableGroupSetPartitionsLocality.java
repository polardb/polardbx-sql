package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Created by taojinkun.
 *
 * @author taojinkun
 */
public class AlterTableGroupSetPartitionsLocality extends DDL {
    final String tableGroupName;
    final String targetLocality;
    final String partition;
    final Boolean isLogical;
    protected AlterTableGroupSetPartitionsLocality(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                   RelDataType rowType,
                                                   String tableGroupName,
                                                   String partition,
                                                   String targetLocality,
                                                   Boolean isLogical) {
        super(cluster, traits, ddl, rowType);
        this.tableGroupName = tableGroupName;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(tableGroupName, SqlParserPos.ZERO));
        this.partition = partition;
        this.targetLocality = targetLocality;
        this.isLogical = isLogical;

    }

    public static AlterTableGroupSetPartitionsLocality create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                              RelDataType rowType,
                                                              String tableGroupName,
                                                              String partition,
                                                              String targetLocality,
                                                              Boolean isLogical) {

        return new AlterTableGroupSetPartitionsLocality(cluster, traits, ddl, rowType,tableGroupName, partition, targetLocality, isLogical);
    }

    @Override
    public AlterTableGroupSetPartitionsLocality copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableGroupSetPartitionsLocality(this.getCluster(), traitSet, this.ddl, rowType, tableGroupName, partition, targetLocality, isLogical);
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public String getTargetLocality(){
        return targetLocality;
    }

    public String getPartition(){
        return partition;
    }

    public Boolean getLogical() {
        return isLogical;
    }
}
