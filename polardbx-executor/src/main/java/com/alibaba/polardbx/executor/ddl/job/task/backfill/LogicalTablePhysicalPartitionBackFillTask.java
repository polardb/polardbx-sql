package com.alibaba.polardbx.executor.ddl.job.task.backfill;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.GsiPartitionBackfill;
import lombok.Getter;

import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.CN_CPU;
import static com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils.CN_NETWORK;

@TaskName(name = "LogicalTablePhysicalPartitionBackFillTask")
@Getter
public class LogicalTablePhysicalPartitionBackFillTask extends BaseBackfillTask implements RemoteExecutableDdlTask {

    public String sourceTableName;
    public String targetTableName;
    public Map<String, String> virtualColumns;
    public Map<String, String> backfillColumnMap;
    public List<String> modifyStringColumns;
    public boolean useChangeSet;
    public boolean modifyColumn;
    public boolean mirrorCopy;
    public List<String> physicalPartitions;
    public int cpuAcquired;

    @JSONCreator
    public LogicalTablePhysicalPartitionBackFillTask(String schemaName,
                                                     String sourceTableName,
                                                     String targetTableName,
                                                     Map<String, String> virtualColumns,
                                                     Map<String, String> backfillColumnMap,
                                                     List<String> modifyStringColumns,
                                                     boolean useChangeSet,
                                                     boolean mirrorCopy,
                                                     boolean modifyColumn,
                                                     List<String> physicalPartitions,
                                                     int cpuAcquired) {
        super(schemaName);
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.virtualColumns = virtualColumns;
        this.backfillColumnMap = backfillColumnMap;
        this.modifyStringColumns = modifyStringColumns;
        this.useChangeSet = useChangeSet;
        this.modifyColumn = modifyColumn;
        this.mirrorCopy = mirrorCopy;
        this.physicalPartitions = physicalPartitions;
        this.cpuAcquired = cpuAcquired;
        setResourceAcquired(buildResourceRequired());
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        executionContext = executionContext.copy();
        executionContext.setBackfillId(getTaskId());
        executionContext.setTaskId(getTaskId());
        GsiPartitionBackfill backFillPlan =
            GsiPartitionBackfill.createGsiPartitionBackfill(schemaName, sourceTableName, targetTableName,
                executionContext);
        backFillPlan.setUseChangeSet(useChangeSet);
        backFillPlan.setOnlineModifyColumn(modifyColumn);
        backFillPlan.setMirrorCopy(mirrorCopy);
        backFillPlan.setModifyStringColumns(modifyStringColumns);
        backFillPlan.setSrcCheckColumnMap(backfillColumnMap);
        backFillPlan.setDstCheckColumnMap(virtualColumns);
        backFillPlan.setPartitionList(physicalPartitions);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        ExecutorHelper.execute(backFillPlan, executionContext);
    }

    DdlEngineResources buildResourceRequired() {
        DdlEngineResources resourceRequired = new DdlEngineResources();
        String owner =
            "LogicalBackfill:" + sourceTableName + ": " + physicalPartitions;
        resourceRequired.request(CN_NETWORK, 5L, owner);
        resourceRequired.request(CN_CPU, Long.valueOf(cpuAcquired), owner);
        return resourceRequired;
    }

    @Override
    public DdlEngineResources getDdlEngineResources() {
        return this.resourceAcquired;
    }

    @Override
    public String remark() {
        return "|logical backfill for table:" + sourceTableName + " |partition:(" + physicalPartitions
            + ")";
    }

}
