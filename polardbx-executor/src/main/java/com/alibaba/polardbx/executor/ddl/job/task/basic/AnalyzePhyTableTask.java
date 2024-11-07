package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.spi.IDataSourceGetter;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.repo.mysql.spi.DatasourceMySQLImplement;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "AnalyzePhyTableTask")
public class AnalyzePhyTableTask extends AnalyzeTablePhyDdlTask {

    private String schemaName;
    private String groupKey;
    private String phyTableName;

    public AnalyzePhyTableTask(String schemaName, String groupKey, String phyTableName) {
        super(ImmutableList.of(schemaName), ImmutableList.of(phyTableName), null, null);
        this.schemaName = schemaName;
        this.groupKey = groupKey;
        this.phyTableName = phyTableName;
        onExceptionTryRecoveryThenPause();
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        ParamManager paramManager = OptimizerContext.getContext(schemaName).getParamManager();
        boolean isAnalyzeTableAfterImportTablespace =
            paramManager.getBoolean(ConnectionParams.ANALYZE_TABLE_AFTER_IMPORT_TABLESPACE);
        if (isAnalyzeTableAfterImportTablespace) {
            IDataSourceGetter mysqlDsGetter = new DatasourceMySQLImplement(schemaName);
            doAnalyzeOnePhysicalTable(groupKey, phyTableName, mysqlDsGetter);
        }

    }
}
