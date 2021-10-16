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

package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.DropGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.DropPartitionGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Getter
@TaskName(name = "DropPartitionGsiPhyDdlTask")
public class DropPartitionGsiPhyDdlTask extends BasePhyDdlTask {

    private final String primaryTableName;
    private final String indexTableName;

    @JSONCreator
    public DropPartitionGsiPhyDdlTask(String schemaName, String primaryTableName, String indexTableName,
                                      PhysicalPlanData physicalPlanData) {
        super(schemaName, physicalPlanData);
        this.primaryTableName = primaryTableName;
        this.indexTableName = indexTableName;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        if (this.physicalPlanData == null) {
            DropGlobalIndexBuilder builder = DropPartitionGlobalIndexBuilder
                .createBuilder(schemaName, primaryTableName, indexTableName, executionContext);
            builder.build();
            this.physicalPlanData = builder.genPhysicalPlanData();
        }
        super.executeImpl(executionContext);
    }
}