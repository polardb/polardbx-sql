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
import com.alibaba.polardbx.executor.ddl.job.builder.DropPartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
@TaskName(name = "DropTruncateTmpPrimaryTablePhyDdlTask")
public class DropTruncateTmpPrimaryTablePhyDdlTask extends BasePhyDdlTask {

    private final String primaryTableName;

    @JSONCreator
    public DropTruncateTmpPrimaryTablePhyDdlTask(String schemaName,
                                                 String primaryTableName) {
        super(schemaName, null);
        this.primaryTableName = primaryTableName;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        DdlPhyPlanBuilder builder = isNewPartDb ?
            DropPartitionTableBuilder.createBuilder(schemaName, primaryTableName, false, executionContext).build() :
            DropTableBuilder.createBuilder(schemaName, primaryTableName, false, executionContext).build();

        Map<String, List<List<String>>> tableTopology = builder.getTableTopology();
        List<PhyDdlTableOperation> physicalPlans = builder.getPhysicalPlans();
        //generate a "drop table" physical plan
        PhysicalPlanData physicalPlanData = DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, physicalPlans);
        this.physicalPlanData = physicalPlanData;

        super.executeImpl(executionContext);
    }

}