package com.alibaba.polardbx.optimizer.core.rel.ddl;

import org.apache.calcite.rel.ddl.ImportSequence;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlImportSequence;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalImportSequence extends BaseDdlOperation {
    protected SqlImportSequence sqlImportSequence;

    public LogicalImportSequence(ImportSequence importSequence) {
        super(importSequence.getCluster(), importSequence.getTraitSet(), importSequence);
        this.sqlImportSequence = (SqlImportSequence) importSequence.sqlNode;
        this.setSchemaName(sqlImportSequence.getLogicalDb());
        this.setTableName("nonsense");
        this.relDdl.setTableName(new SqlIdentifier("nonsense", SqlParserPos.ZERO));
    }

    public static LogicalImportSequence create(ImportSequence importSequence) {
        return new LogicalImportSequence(importSequence);
    }
}
