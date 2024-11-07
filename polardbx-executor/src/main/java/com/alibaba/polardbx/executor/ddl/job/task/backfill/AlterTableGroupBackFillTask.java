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

package com.alibaba.polardbx.executor.ddl.job.task.backfill;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.AlterTableGroupBackfill;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalBackfill;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

import java.util.Map;
import java.util.Set;

@TaskName(name = "AlterTableGroupBackFillTask")
@Getter
public class AlterTableGroupBackFillTask extends BaseBackfillTask implements RemoteExecutableDdlTask {

    String logicalTableName;
    Map<String, Set<String>> sourcePhyTables;
    Map<String, Set<String>> targetPhyTables;
    boolean broadcast;
    boolean movePartitions;
    boolean useChangeSet;
    boolean usePhysicalBackfill;

    @JSONCreator
    public AlterTableGroupBackFillTask(String schemaName,
                                       String logicalTableName,
                                       Map<String, Set<String>> sourcePhyTables,
                                       Map<String, Set<String>> targetPhyTables,
                                       boolean broadcast,
                                       boolean movePartitions,
                                       boolean useChangeSet,
                                       boolean usePhysicalBackfill) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.sourcePhyTables = sourcePhyTables;
        this.targetPhyTables = targetPhyTables;
        this.broadcast = broadcast;
        this.movePartitions = movePartitions;
        this.useChangeSet = useChangeSet;
        this.usePhysicalBackfill = usePhysicalBackfill;
        if (useChangeSet) {
            // onExceptionTryRollback, such as dn ha
            onExceptionTryRecoveryThenRollback();
        }
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        if (usePhysicalBackfill) {
            updateTaskStateInNewTxn(DdlTaskState.DIRTY);
        }
        executeImpl(executionContext);
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
        executionContext = executionContext.copy();
        executionContext.setBackfillId(getTaskId());
        executionContext.setTaskId(getTaskId());
        executionContext.setSchemaName(schemaName);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        if (usePhysicalBackfill && !broadcast) {
            final RelNode executablePhyBackfillPlan =
                PhysicalBackfill.createPhysicalBackfill(schemaName, logicalTableName, executionContext, sourcePhyTables,
                    targetPhyTables, broadcast, null);
            ExecutorHelper.execute(executablePhyBackfillPlan, executionContext);
        } else {
            final RelNode executableLogicalBackfillPlan = AlterTableGroupBackfill
                .createAlterTableGroupBackfill(schemaName, logicalTableName, executionContext, sourcePhyTables,
                    targetPhyTables, broadcast, movePartitions, useChangeSet);
            ExecutorHelper.execute(executableLogicalBackfillPlan, executionContext);
        }
    }

    @Override
    protected void rollbackImpl(ExecutionContext executionContext) {
        if (usePhysicalBackfill) {
            //cleanup idb file
            PhysicalBackfillUtils.rollbackCopyIbd(getTaskId(), schemaName, logicalTableName, 0, executionContext);
        } else {
            GsiBackfillManager gsiBackfillManager = new GsiBackfillManager(schemaName);
            gsiBackfillManager.deleteByBackfillId(getTaskId());
        }
    }

    public static String getTaskName() {
        return "AlterTableGroupBackFillTask";
    }
}
