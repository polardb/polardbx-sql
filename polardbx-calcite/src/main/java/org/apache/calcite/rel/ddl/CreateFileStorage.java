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
import java.util.Map;

public class CreateFileStorage extends DDL {
    private final String engineName;
    private final Map<String, String> with;

    protected CreateFileStorage(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                              RelDataType rowType, String engineName, Map<String, String> with) {
        super(cluster, traits, ddl, rowType);
        this.engineName = engineName;
        this.with = with;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(engineName, SqlParserPos.ZERO));
    }

    public static CreateFileStorage create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                         RelDataType rowType,  String engineName, Map<String, String> with) {

        return new CreateFileStorage(cluster, traits, ddl, rowType, engineName, with);
    }

    @Override
    public CreateFileStorage copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new CreateFileStorage(this.getCluster(), traitSet, this.ddl, rowType, engineName, with);
    }

    public String getEngineName() {
        return engineName;
    }

    public Map<String, String> getWith() {
        return with;
    }
}
