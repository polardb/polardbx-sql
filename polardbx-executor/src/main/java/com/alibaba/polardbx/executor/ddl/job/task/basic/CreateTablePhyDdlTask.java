/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPhyTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

import java.util.List;

@Getter
@TaskName(name = "CreateTablePhyDdlTask")
public class CreateTablePhyDdlTask extends BasePhyDdlTask {

    private String logicalTableName;

    @JSONCreator
    public CreateTablePhyDdlTask(String schemaName, String logicalTableName, PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
        this.logicalTableName = logicalTableName;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected List<RelNode> genRollbackPhysicalPlans(ExecutionContext executionContext) {
        DdlPhyPlanBuilder
            dropPhyTableBuilder = DropPhyTableBuilder
            .createBuilder(schemaName, logicalTableName, true, this.physicalPlanData.getTableTopology(),
                executionContext).build();
        List<PhyDdlTableOperation> physicalPlans = dropPhyTableBuilder.getPhysicalPlans();
        // delete redundant params in foreign key
        physicalPlans.forEach(physicalPlan -> {
            physicalPlan.getParam().entrySet().removeIf(entry -> !entry.getKey().equals(1));
        });
        return convertToRelNodes(physicalPlans);
    }

}
