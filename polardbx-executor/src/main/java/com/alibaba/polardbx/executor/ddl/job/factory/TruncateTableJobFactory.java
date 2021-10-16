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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TruncateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TruncateTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.google.common.collect.Lists;

import java.util.Set;

public class TruncateTableJobFactory extends DdlJobFactory {

    private final PhysicalPlanData physicalPlanData;
    private final String schemaName;
    private final String logicalTableName;

    public TruncateTableJobFactory(PhysicalPlanData physicalPlanData) {
        this.physicalPlanData = physicalPlanData;
        this.schemaName = physicalPlanData.getSchemaName();
        this.logicalTableName = physicalPlanData.getLogicalTableName();
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        DdlTask validateTask = new TruncateTableValidateTask(schemaName, logicalTableName);
        DdlTask phyDdlTask = new TruncateTablePhyDdlTask(schemaName, physicalPlanData);
        DdlTask cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(Lists.newArrayList(
            validateTask,
            phyDdlTask.onExceptionTryRecoveryThenRollback(),
            cdcDdlMarkTask
        ));

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
