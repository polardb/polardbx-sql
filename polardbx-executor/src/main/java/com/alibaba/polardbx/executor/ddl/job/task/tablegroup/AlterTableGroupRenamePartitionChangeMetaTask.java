package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "AlterTableGroupRenamePartitionChangeMetaTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AlterTableGroupRenamePartitionChangeMetaTask extends BaseDdlTask {

    protected String tableGroupName;
    protected List<Pair<String, String>> changePartitionsPair;

    @JSONCreator
    public AlterTableGroupRenamePartitionChangeMetaTask(String schemaName, String tableGroupName,
                                                        List<Pair<String, String>> changePartitionsPair) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
        this.changePartitionsPair = changePartitionsPair;

    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        final TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);

        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        partitionGroupAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor.setConnection(metaDbConnection);
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
        for (Pair<String, String> pair : changePartitionsPair) {
            PartitionGroupRecord partitionGroupRecord =
                partitionGroupRecords.stream().filter(o -> o.partition_name.equalsIgnoreCase(pair.getKey())).findFirst()
                    .orElse(null);
            partitionGroupAccessor.updatePartitioNameById(partitionGroupRecord.id, pair.getValue());
            tablePartitionAccessor
                .updatePartitionNameByGroupId(partitionGroupRecord.id, pair.getKey(), pair.getValue());
        }

        SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
        if (!GeneralUtil.isEmpty(tableGroupConfig.getAllTables())) {
            for (TablePartRecordInfoContext recordInfoContext : tableGroupConfig.getAllTables()) {
                try {
                    String tableName = recordInfoContext.getLogTbRec().tableName;
                    TableMeta tableMeta = schemaManager.getTable(tableName);
                    if (tableMeta.isGsi()) {
                        //all the gsi table version change will be behavior by primary table
                        assert
                            tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                        tableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                    }
                    TableInfoManager.updateTableVersion(schemaName, tableName, metaDbConnection);
                } catch (Throwable t) {
                    LOGGER.error(String.format(
                        "error occurs while update table version, schemaName:%s, tableName:%s",
                        recordInfoContext.getLogTbRec().tableSchema,
                        recordInfoContext.getLogTbRec().tableName));
                    throw GeneralUtil.nestedException(t);
                }
            }
        }
        updateSupportedCommands(true, false, null);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        //ComplexTaskMetaManager.getInstance().reload();
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

}
