package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupRenamePartitionChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupRenamePartitionPreparedData;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * @author luoyanxin
 */
public class AlterTableGroupRenamePartitionJobFactory extends DdlJobFactory {

    @Deprecated
    protected final DDL ddl;
    protected final AlterTableGroupRenamePartitionPreparedData preparedData;
    protected final ExecutionContext executionContext;

    public AlterTableGroupRenamePartitionJobFactory(DDL ddl, AlterTableGroupRenamePartitionPreparedData preparedData,
                                                    ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.ddl = ddl;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
        Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);

        Map<String, Long> tablesVersion = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        List<String> logicalTableNames = new ArrayList<>();
        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager()
                .getTableGroupConfigByName(preparedData.getTableGroupName());
        for (TablePartRecordInfoContext tablePartRecordInfoContext : tableGroupConfig.getAllTables()) {
            String tableName = tablePartRecordInfoContext.getLogTbRec().getTableName();
            String primaryTableName;
            TableMeta tableMeta = executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(tableName);
            if (tableMeta.isGsi()) {
                //all the gsi table version change will be behavior by primary table
                assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                tableMeta = executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(primaryTableName);
            } else {
                primaryTableName = tableName;
            }
            logicalTableNames.add(primaryTableName);
            tablesVersion.put(primaryTableName, tableMeta.getVersion());
        }

        DdlTask changeMetaTask = new AlterTableGroupRenamePartitionChangeMetaTask(preparedData.getSchemaName(),
            preparedData.getTableGroupName(), preparedData.getChangePartitionsPair());
        DdlTask syncTask = new TablesSyncTask(preparedData.getSchemaName(), logicalTableNames, true, initWait, interval,
            TimeUnit.MILLISECONDS);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        DdlTask validateTask =
            new AlterTableGroupValidateTask(preparedData.getSchemaName(), preparedData.getTableGroupName(),
                tablesVersion,
                true, null);

        DdlTask reloadTableGroup =
            new TableGroupSyncTask(preparedData.getSchemaName(), preparedData.getTableGroupName());
        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            changeMetaTask,
            syncTask,
            reloadTableGroup
        ));
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          AlterTableGroupRenamePartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new AlterTableGroupRenamePartitionJobFactory(ddl, preparedData, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
        for (String relatedPart : preparedData.getRelatedPartitions()) {
            resources.add(concatWithDot(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()),
                relatedPart));
        }
        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager()
                .getTableGroupConfigByName(preparedData.getTableGroupName());
        for (TablePartRecordInfoContext tablePartRecordInfoContext : tableGroupConfig.getAllTables()) {
            String tableName = tablePartRecordInfoContext.getLogTbRec().getTableName();
            resources.add(concatWithDot(preparedData.getSchemaName(), tableName));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
