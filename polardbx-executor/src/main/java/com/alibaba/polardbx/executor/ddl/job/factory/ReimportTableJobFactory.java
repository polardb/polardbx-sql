package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.ReimportTableChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ReimportTableJobFactory extends CreatePartitionTableJobFactory {

    public ReimportTableJobFactory(boolean autoPartition, boolean hasTimestampColumnDefault,
                                   Map<String, String> specialDefaultValues,
                                   Map<String, Long> specialDefaultValueFlags,
                                   List<ForeignKeyData> addedForeignKeys,
                                   PhysicalPlanData physicalPlanData, ExecutionContext executionContext,
                                   CreateTablePreparedData preparedData, PartitionInfo partitionInfo) {
        super(autoPartition, hasTimestampColumnDefault, specialDefaultValues, specialDefaultValueFlags,
            addedForeignKeys,
            physicalPlanData, executionContext, preparedData, partitionInfo, null);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ReimportTableChangeMetaTask reimportTableChangeMetaTask =
            new ReimportTableChangeMetaTask(schemaName, logicalTableName, physicalPlanData.getDefaultDbIndex(),
                physicalPlanData.getDefaultPhyTableName(), physicalPlanData.getSequence(),
                physicalPlanData.getTablesExtRecord(), physicalPlanData.isPartitioned(),
                physicalPlanData.isIfNotExists(), physicalPlanData.getKind(), addedForeignKeys,
                hasTimestampColumnDefault,
                specialDefaultValues, specialDefaultValueFlags);

        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(ImmutableList.of(
            reimportTableChangeMetaTask,
            tableSyncTask
        ));

        return executableDdlJob;
    }
}
