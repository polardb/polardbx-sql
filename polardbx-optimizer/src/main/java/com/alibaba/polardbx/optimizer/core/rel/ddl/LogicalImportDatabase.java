package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import org.apache.calcite.rel.ddl.AlterDatabase;
import org.apache.calcite.rel.ddl.ImportDatabase;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlImportDatabase;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalImportDatabase extends BaseDdlOperation {
    protected SqlImportDatabase sqlImportDatabase;

    public LogicalImportDatabase(ImportDatabase importDatabase) {
        super(importDatabase.getCluster(), importDatabase.getTraitSet(), importDatabase);
        this.sqlImportDatabase = (SqlImportDatabase) importDatabase.sqlNode;
        this.setSchemaName(DefaultDbSchema.NAME);
        this.setTableName("nonsense");
        this.relDdl.setTableName(new SqlIdentifier("nonsense", SqlParserPos.ZERO));
    }

    public static LogicalImportDatabase create(ImportDatabase importDatabase) {
        return new LogicalImportDatabase(importDatabase);
    }

}
