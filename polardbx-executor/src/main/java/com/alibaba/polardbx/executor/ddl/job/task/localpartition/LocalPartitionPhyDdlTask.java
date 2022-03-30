package com.alibaba.polardbx.executor.ddl.job.task.localpartition;

import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import lombok.Getter;

@Getter
@TaskName(name = "LocalPartitionPhyDdlTask")
public class LocalPartitionPhyDdlTask extends BasePhyDdlTask {

    public LocalPartitionPhyDdlTask(String schemaName,
                                    PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
        onExceptionTryRecoveryThenPause();
    }
}