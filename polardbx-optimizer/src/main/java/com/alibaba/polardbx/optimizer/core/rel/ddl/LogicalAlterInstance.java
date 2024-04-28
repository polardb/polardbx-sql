package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import org.apache.calcite.rel.ddl.AlterInstance;
import org.apache.calcite.sql.SqlAlterInstance;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalAlterInstance extends BaseDdlOperation {
    protected SqlAlterInstance sqlAlterInstance;

    public LogicalAlterInstance(AlterInstance alterDatabase) {
        super(alterDatabase.getCluster(), alterDatabase.getTraitSet(), alterDatabase);
        this.sqlAlterInstance = (SqlAlterInstance) alterDatabase.sqlNode;
        this.setSchemaName(DefaultDbSchema.NAME);
        this.setTableName("nonsense");
        this.relDdl.setTableName(new SqlIdentifier("nonsense", SqlParserPos.ZERO));
    }

    public static LogicalAlterInstance create(AlterInstance alterInstance) {
        return new LogicalAlterInstance(alterInstance);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }
}
