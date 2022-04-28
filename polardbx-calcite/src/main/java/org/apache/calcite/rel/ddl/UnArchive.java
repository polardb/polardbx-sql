package org.apache.calcite.rel.ddl;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlUnArchive;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * @author Shi Yuxuan
 */
public class UnArchive extends DDL {

    protected UnArchive(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl, RelDataType rowType) {
        super(cluster, traits, ddl, rowType);
        this.sqlNode = ddl;
        SqlUnArchive sqlUnArchive = (SqlUnArchive)ddl;
        switch (sqlUnArchive.getTarget()) {
        case TABLE:
        case TABLE_GROUP:
            this.setTableName(sqlUnArchive.getNode());
            break;
        case DATABASE:
            this.setTableName(new SqlIdentifier("-", SqlParserPos.ZERO));
        }
    }
    public static UnArchive create(SqlUnArchive sqlUnArchive, RelDataType rowType, RelOptCluster cluster) {
        return new UnArchive(cluster, cluster.traitSetOf(Convention.NONE), sqlUnArchive, rowType);
    }
    @Override
    public UnArchive copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new UnArchive(this.getCluster(), traitSet, ((UnArchive) inputs.get(0)).getAst(), rowType);
    }
}
