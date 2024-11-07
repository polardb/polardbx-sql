package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPhyTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
@TaskName(name = "CreatePhyTableWithRollbackCheckTask")
public class CreatePhyTableWithRollbackCheckTask extends CreateTablePhyDdlTask {

    Map<String, Set<String>> sourceTableTopology;

    @JSONCreator
    public CreatePhyTableWithRollbackCheckTask(String schemaName, String logicalTableName,
                                               PhysicalPlanData physicalPlanData,
                                               Map<String, Set<String>> sourceTableTopology) {
        super(schemaName, logicalTableName, physicalPlanData);
        this.sourceTableTopology = sourceTableTopology;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected List<RelNode> genRollbackPhysicalPlans(ExecutionContext executionContext) {
        for (Map.Entry<String, Set<String>> entry : sourceTableTopology.entrySet()) {
            for (String phyTb : entry.getValue()) {
                if (!ScaleOutUtils.checkTableExistence(schemaName, entry.getKey(), phyTb)) {
                    throw GeneralUtil.nestedException(
                        "The source table '" + phyTb + "' does not exist in '" + entry.getKey()
                            + "'; therefore, this DDL operation cannot be rolled back.");
                }
            }
        }
        return super.genRollbackPhysicalPlans(executionContext);
    }

}
