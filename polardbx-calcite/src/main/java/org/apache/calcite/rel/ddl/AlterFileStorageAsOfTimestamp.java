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

public class AlterFileStorageAsOfTimestamp extends DDL {
    final String fileStorageName;
    final String timestamp;

    protected AlterFileStorageAsOfTimestamp(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                            RelDataType rowType, String fileStorageName, String timestamp) {
        super(cluster, traits, ddl, rowType);
        this.fileStorageName = fileStorageName;
        this.timestamp = timestamp;
        this.sqlNode = ddl;
        this.setTableName(new SqlIdentifier(fileStorageName, SqlParserPos.ZERO));
    }

    public static AlterFileStorageAsOfTimestamp create(RelOptCluster cluster, RelTraitSet traits, SqlDdl ddl,
                                                       RelDataType rowType, String fileStorageName, String timestamp) {

        return new AlterFileStorageAsOfTimestamp(cluster, traits, ddl, rowType, fileStorageName, timestamp);
    }

    @Override
    public AlterFileStorageAsOfTimestamp copy(
            RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new AlterFileStorageAsOfTimestamp(this.getCluster(), traitSet, this.ddl, rowType, fileStorageName, timestamp);
    }

    public String getFileStorageName() {
        return fileStorageName;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
