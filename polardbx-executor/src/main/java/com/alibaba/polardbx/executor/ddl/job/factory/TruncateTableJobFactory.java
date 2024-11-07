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
import com.alibaba.polardbx.executor.ddl.job.task.basic.ResetSequence4TruncateTableTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TruncateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TruncateTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.TruncateColumnarTableTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TruncateTableJobFactory extends DdlJobFactory {

    private final PhysicalPlanData physicalPlanData;
    private final String schemaName;
    private final String logicalTableName;
    private final long tableVersion;
    private final long versionId;
    private final boolean withCci;

    public TruncateTableJobFactory(PhysicalPlanData physicalPlanData, long tableVersion, long versionId,
                                   boolean withCci) {
        this.physicalPlanData = physicalPlanData;
        this.schemaName = physicalPlanData.getSchemaName();
        this.logicalTableName = physicalPlanData.getLogicalTableName();
        this.tableVersion = tableVersion;
        this.versionId = versionId;
        this.withCci = withCci;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        TableGroupConfig tableGroupConfig = isNewPart ? physicalPlanData.getTableGroupConfig() : null;

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(logicalTableName, tableVersion);
        ValidateTableVersionTask validateTableVersionTask = new ValidateTableVersionTask(schemaName, tableVersions);

        DdlTask validateTask = new TruncateTableValidateTask(schemaName, logicalTableName, tableGroupConfig);
        DdlTask phyDdlTask = new TruncateTablePhyDdlTask(schemaName, physicalPlanData);
        DdlTask resetSequenceTask = new ResetSequence4TruncateTableTask(schemaName, logicalTableName);
        DdlTask truncateColumnarTableTask = new TruncateColumnarTableTask(schemaName, logicalTableName, versionId);
        DdlTask cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData, false, false, versionId);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(
            !withCci ?
                Lists.newArrayList(
                    validateTableVersionTask,
                    validateTask,
                    resetSequenceTask,
                    phyDdlTask.onExceptionTryRecoveryThenRollback(),
                    cdcDdlMarkTask
                ) :
                Lists.newArrayList(
                    validateTableVersionTask,
                    validateTask,
                    resetSequenceTask,
                    phyDdlTask.onExceptionTryRecoveryThenRollback(),
                    truncateColumnarTableTask,
                    cdcDdlMarkTask
                )
        );

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
