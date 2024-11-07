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

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AddLogicalForeignKeyTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreatePhyTableWithRollbackCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropLogicalForeignKeyTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.MoveDatabaseAddMetaTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.MoveDatabaseItemPreparedData;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class MoveDatabaseSubTaskJobFactory extends DdlJobFactory {

    @Deprecated
    protected final DDL ddl;
    protected final MoveDatabaseItemPreparedData preparedData;
    protected final List<PhyDdlTableOperation> phyDdlTableOperations;
    protected final Map<String, List<List<String>>> tableTopology;
    protected final Map<String, Set<String>> targetTableTopology;
    protected final Map<String, Set<String>> sourceTableTopology;
    protected final ExecutionContext executionContext;

    public MoveDatabaseSubTaskJobFactory(DDL ddl, MoveDatabaseItemPreparedData preparedData,
                                         List<PhyDdlTableOperation> phyDdlTableOperations,
                                         Map<String, List<List<String>>> tableTopology,
                                         Map<String, Set<String>> targetTableTopology,
                                         Map<String, Set<String>> sourceTableTopology,
                                         ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.phyDdlTableOperations = phyDdlTableOperations;
        this.ddl = ddl;
        this.tableTopology = tableTopology;
        this.targetTableTopology = targetTableTopology;
        this.sourceTableTopology = sourceTableTopology;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

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

        //1. add logical foreign key
        DdlTask addLogicalForeignKeyTask = getPushDownForeignKeysTask(schemaName, tableName, true);
        taskList.add(addLogicalForeignKeyTask);

        //2. create physical table
        //2.1 insert meta to complex_task_outline
        taskList.add(addMetaTask);
        //2.2 create partitioned physical table
        PhysicalPlanData physicalPlanData =
            DdlJobDataConverter.convertToPhysicalPlanData(tableTopology, phyDdlTableOperations, executionContext);
        DdlTask phyDdlTask =
            new CreatePhyTableWithRollbackCheckTask(schemaName, physicalPlanData.getLogicalTableName(),
                physicalPlanData, sourceTableTopology);
        taskList.add(phyDdlTask);

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.SCALE_OUT_FINAL_TABLE_STATUS_DEBUG);
        final boolean stayAtCreating =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.CREATING.name(), finalStatus);
        final boolean stayAtDeleteOnly =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY.name(), finalStatus);
        final boolean stayAtWriteOnly =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY.name(), finalStatus);
        final boolean stayAtWriteReOrg =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG.name(), finalStatus);

        List<DdlTask> bringUpNewPartitions = ComplexTaskFactory
            .moveTableTasks(schemaName, tableName, sourceTableTopology, targetTableTopology,
                preparedData.getSourceTargetGroupMap(), stayAtCreating, stayAtDeleteOnly, stayAtWriteOnly,
                stayAtWriteReOrg, executionContext);
        //3.2 status: CREATING -> DELETE_ONLY -> WRITE_ONLY -> WRITE_REORG -> READY_TO_PUBLIC
        taskList.addAll(bringUpNewPartitions);

        // drop logical foreign key
        DdlTask dropLogicalForeignKeyTask = getPushDownForeignKeysTask(schemaName, tableName, false);
        taskList.add(dropLogicalForeignKeyTask);

        //todo(ziyang) cdc ddl mark task

        final ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.labelAsHead(addLogicalForeignKeyTask);
        if (!stayAtCreating) {
            executableDdlJob.labelAsTail(bringUpNewPartitions.get(bringUpNewPartitions.size() - 1));
        } else {
            executableDdlJob.labelAsTail(phyDdlTask);
        }
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        for (String groupName : preparedData.getSourceTargetGroupMap().keySet()) {
            resources.add(
                concatWithDot(concatWithDot(preparedData.getSchemaName(), groupName), preparedData.getTableName()));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    DdlTask getPushDownForeignKeysTask(String schemaName, String tableName, boolean add) {
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        List<ForeignKeyData> pushDownForeignKeys = new ArrayList<>(tableMeta.getForeignKeys().values());

        if (add) {
            return new AddLogicalForeignKeyTask(schemaName, tableName, pushDownForeignKeys);
        } else {
            return new DropLogicalForeignKeyTask(schemaName, tableName, pushDownForeignKeys);
        }
    }

    public List<DdlTask> getBackfillTaskEdgeNodes() {
        return null;
    }

    public List<List<DdlTask>> getPhysicalyTaskPipeLine() {
        return null;
    }
}
