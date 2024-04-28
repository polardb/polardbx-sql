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

public class ClearFileStorage extends DDL {
    final String fileStorageName;

    protected ClearFileStorage(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                               RelDataType rowType, String fileStorageName) {
        super(cluster, traits, ddl, rowType);
        this.fileStorageName = fileStorageName;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(fileStorageName, SqlParserPos.ZERO));
    }

    public static ClearFileStorage create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                          RelDataType rowType, String fileStorageName) {
        return new ClearFileStorage(cluster, traits, ddl, rowType, fileStorageName);
    }

    @Override
    public ClearFileStorage copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new ClearFileStorage(this.getCluster(), traitSet, this.ddl, rowType, fileStorageName);
    }

    public String getFileStorageName() {
        return fileStorageName;
    }
}
