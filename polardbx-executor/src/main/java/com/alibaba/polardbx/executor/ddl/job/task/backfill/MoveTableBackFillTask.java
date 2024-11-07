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
import com.alibaba.polardbx.executor.physicalbackfill.PhysicalBackfillUtils;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalBackfill;
import com.alibaba.polardbx.optimizer.core.rel.MoveTableBackfill;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

import java.util.Map;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
@TaskName(name = "MoveTableBackFillTask")
@Getter
public class MoveTableBackFillTask extends BaseBackfillTask implements RemoteExecutableDdlTask {

    String logicalTableName;
    Map<String, Set<String>> sourcePhyTables;
    Map<String, Set<String>> targetPhyTables;
    Map<String, String> sourceTargetGroup;
    boolean broadcast;
    boolean useChangeSet;

    @JSONCreator
    public MoveTableBackFillTask(String schemaName,
                                 String logicalTableName,
                                 Map<String, Set<String>> sourcePhyTables,
                                 Map<String, Set<String>> targetPhyTables,
                                 Map<String, String> sourceTargetGroup,
                                 boolean broadcast,
                                 boolean useChangeSet) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.sourcePhyTables = sourcePhyTables;
        this.targetPhyTables = targetPhyTables;
        this.sourceTargetGroup = sourceTargetGroup;
        this.broadcast = broadcast;
        this.useChangeSet = useChangeSet;
        if (useChangeSet) {
            // onExceptionTryRollback, such as dn ha
            onExceptionTryRecoveryThenRollback();
        }
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
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
        final MoveTableBackfill backFillPlan =
            MoveTableBackfill
                .createMoveTableBackfill(schemaName, logicalTableName, executionContext, sourcePhyTables,
                    targetPhyTables, sourceTargetGroup, useChangeSet);
        ExecutorHelper.execute(backFillPlan, executionContext);
    }

    @Override
    protected void rollbackImpl(ExecutionContext executionContext) {
        GsiBackfillManager gsiBackfillManager = new GsiBackfillManager(schemaName);
        gsiBackfillManager.deleteByBackfillId(getTaskId());
    }

    public static String getTaskName() {
        return "MoveTableBackFillTask";
    }

}
