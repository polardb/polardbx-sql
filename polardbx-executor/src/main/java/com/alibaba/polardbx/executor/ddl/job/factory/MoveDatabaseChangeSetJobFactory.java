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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.MoveTableBackFillTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.MoveDatabaseAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyExecutorInitTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyFinishTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.MoveTableCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetCatchUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetStartTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MoveDatabaseItemPreparedData;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.common.properties.ConnectionParams.CHANGE_SET_APPLY_OPTIMIZATION;
import static com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils.genChangeSetCatchUpTasks;

public class MoveDatabaseChangeSetJobFactory extends MoveDatabaseSubTaskJobFactory {
    private ChangeSetApplyExecutorInitTask changeSetApplyExecutorInitTask;
    private ChangeSetApplyFinishTask changeSetApplyFinishTask;

    public MoveDatabaseChangeSetJobFactory(DDL ddl, MoveDatabaseItemPreparedData preparedData,
                                           List<PhyDdlTableOperation> phyDdlTableOperations,
                                           Map<String, List<List<String>>> tableTopology,
                                           Map<String, Set<String>> targetTableTopology,
                                           Map<String, Set<String>> sourceTableTopology,
                                           ChangeSetApplyExecutorInitTask changeSetApplyExecutorInitTask,
                                           ChangeSetApplyFinishTask changeSetApplyFinishTask,
                                           ExecutionContext executionContext) {
        super(ddl, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology, sourceTableTopology,
            executionContext);
        this.changeSetApplyExecutorInitTask = changeSetApplyExecutorInitTask;
        this.changeSetApplyFinishTask = changeSetApplyFinishTask;
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();

        DdlTask addMetaTask =
            new MoveDatabaseAddMetaTask(schemaName, ImmutableList.of(tableName), "",
                ComplexTaskMetaManager.ComplexTaskStatus.CREATING.getValue(),
                ComplexTaskMetaManager.ComplexTaskType.MOVE_DATABASE.getValue(),
                1);

        List<DdlTask> taskList = new ArrayList<>();
        //1. validate
        //taskList.add(validateTask);
        if (changeSetApplyExecutorInitTask != null) {
            taskList.add(changeSetApplyExecutorInitTask);
        }

        //2. create physical table
        //2.1 insert meta to complex_task_outline
        taskList.add(addMetaTask);
        //2.2 create partitioned physical table
        PhysicalPlanData physicalPlanData =
            DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, phyDdlTableOperations);
        DdlTask phyDdlTask =
            new CreateTablePhyDdlTask(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData);
        taskList.add(phyDdlTask);

        List<String> relatedTables = new ArrayList<>();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        if (tableMeta.isGsi()) {
            //all the gsi table version change will be behavior by primary table
            assert tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
            relatedTables.add(tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName);
        } else {
            relatedTables.add(tableName);
        }

        MoveTableBackFillTask moveTableBackFillTask =
            new MoveTableBackFillTask(schemaName, tableName, sourceTableTopology, targetTableTopology,
                preparedData.getSourceTargetGroupMap(), true);

        Long changeSetId = ChangeSetManager.getChangeSetId();

        ChangeSetStartTask changeSetStartTask = new ChangeSetStartTask(
            schemaName,
            tableName,
            sourceTableTopology,
            ComplexTaskMetaManager.ComplexTaskType.MOVE_DATABASE,
            changeSetId
        );

        Map<String, ChangeSetCatchUpTask> catchUpTasks = genChangeSetCatchUpTasks(
            schemaName,
            tableName,
            sourceTableTopology,
            preparedData.getSourceTargetGroupMap(),
            ComplexTaskMetaManager.ComplexTaskType.MOVE_DATABASE,
            changeSetId
        );

        final boolean useApplyOpt = changeSetApplyFinishTask != null
            && executionContext.getParamManager().getBoolean(CHANGE_SET_APPLY_OPTIMIZATION);
        MoveTableCheckTask moveTableCheckTask =
            new MoveTableCheckTask(schemaName, tableName, preparedData.getSourceTargetGroupMap(), sourceTableTopology,
                targetTableTopology, useApplyOpt, relatedTables);
        MoveTableCheckTask moveTableCheckTwiceTask =
            new MoveTableCheckTask(schemaName, tableName, preparedData.getSourceTargetGroupMap(), sourceTableTopology,
                targetTableTopology, false, relatedTables);

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.SCALE_OUT_FINAL_TABLE_STATUS_DEBUG);

        final ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> moveDatabaseTasks = ChangeSetUtils.genChangeSetOnlineSchemaChangeTasks(
            schemaName, tableName,
            relatedTables,
            finalStatus,
            changeSetStartTask,
            catchUpTasks,
            moveTableBackFillTask,
            moveTableCheckTask,
            moveTableCheckTwiceTask,
            changeSetApplyFinishTask,
            executionContext);

        taskList.addAll(moveDatabaseTasks);
        executableDdlJob.addSequentialTasks(taskList);

        //todo(ziyang) cdc ddl mark task
        if (changeSetApplyExecutorInitTask != null) {
            executableDdlJob.labelAsHead(changeSetApplyExecutorInitTask);
        } else {
            executableDdlJob.labelAsHead(addMetaTask);
        }

        executableDdlJob.labelAsTail(taskList.get(taskList.size() - 1));

        return executableDdlJob;
    }
}
