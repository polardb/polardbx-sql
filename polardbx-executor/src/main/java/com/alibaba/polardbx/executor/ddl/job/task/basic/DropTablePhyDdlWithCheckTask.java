package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.Map;
import java.util.Set;

@TaskName(name = "DropTablePhyDdlWithCheckTask")
public class DropTablePhyDdlWithCheckTask extends DropTablePhyDdlTask {

    Map<String, Set<String>> targetTableTopology;

    @JSONCreator
    public DropTablePhyDdlWithCheckTask(String schemaName, PhysicalPlanData physicalPlanData,
                                        Map<String, Set<String>> targetTableTopology) {
        super(schemaName, physicalPlanData);
        this.targetTableTopology = targetTableTopology;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {

        super.executeImpl(executionContext);
    }
}
