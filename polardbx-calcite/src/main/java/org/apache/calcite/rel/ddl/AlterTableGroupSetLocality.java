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
public class AlterTableGroupSetLocality extends DDL {
    final String tableGroupName;
    final String targetLocality;
    final Boolean isLogical;

    protected AlterTableGroupSetLocality(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                         RelDataType rowType,
                                         String tableGroupName,
                                         String targetLocality,
                                         Boolean isLogical) {
        super(cluster, traits, ddl, rowType);
        this.tableGroupName = tableGroupName;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(tableGroupName, SqlParserPos.ZERO));
        this.targetLocality = targetLocality;
        this.isLogical = isLogical;

    }

    public static AlterTableGroupSetLocality create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                    RelDataType rowType,
                                                    String tableGroupName,
                                                    String targetLocality,
                                                    Boolean isLogical) {

        return new AlterTableGroupSetLocality(cluster, traits, ddl, rowType,tableGroupName, targetLocality, isLogical);
    }

    @Override
    public AlterTableGroupSetLocality copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterTableGroupSetLocality(this.getCluster(), traitSet, this.ddl, rowType, tableGroupName, targetLocality, isLogical);
    }

    public String getTableGroupName() {
        return tableGroupName;
    }

    public String getTargetLocality(){
        return targetLocality;
    }

    public Boolean getLogical() {
        return isLogical;
    }
}
