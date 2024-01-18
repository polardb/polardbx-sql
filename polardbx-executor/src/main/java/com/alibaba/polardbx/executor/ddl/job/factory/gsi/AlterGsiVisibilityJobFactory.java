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

package com.alibaba.polardbx.executor.ddl.job.factory.gsi;

import com.alibaba.polardbx.executor.ddl.job.task.AlterGsiVisibilityValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiUpdateIndexStatusTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiUpdateIndexVisibilityTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterGlobalIndexVisibilityPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class AlterGsiVisibilityJobFactory extends DdlJobFactory {
    protected final String schemaName;
    protected final String primaryTableName;
    protected final String indexTableName;
    protected final String visibility;
    protected final AlterGlobalIndexVisibilityPreparedData preparedData;
    protected final ExecutionContext executionContext;

    public AlterGsiVisibilityJobFactory(AlterTableWithGsiPreparedData alterTableWithGsiPreparedData,
                                        ExecutionContext executionContext) {
        this.preparedData = alterTableWithGsiPreparedData.getGlobalIndexVisibilityPreparedData();
        this.schemaName = preparedData.getSchemaName();
        this.primaryTableName = preparedData.getPrimaryTableName();
        this.indexTableName = preparedData.getIndexTableName();
        this.visibility = preparedData.getVisibility();
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        GsiValidator.validateGsi(schemaName, indexTableName);
        GsiValidator.validateAllowDdlOnTable(schemaName, primaryTableName, executionContext);
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        resources.add(concatWithDot(schemaName, indexTableName));

        //lock tablegroup of index table
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            OptimizerContext oc =
                Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
            PartitionInfo partitionInfo = oc.getPartitionInfoManager().getPartitionInfo(indexTableName);
            if (partitionInfo != null && partitionInfo.getTableGroupId() != -1) {
                TableGroupConfig tableGroupConfig =
                    oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
                String tgName = tableGroupConfig.getTableGroupRecord().getTg_name();
                resources.add(concatWithDot(schemaName, tgName));
            }
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        AlterGsiVisibilityValidateTask validateTask =
            new AlterGsiVisibilityValidateTask(schemaName, primaryTableName, indexTableName, visibility);

        Map<String, Long> tableVersions = ImmutableMap.of(
            indexTableName, preparedData.getTableVersion()
        );
        ValidateTableVersionTask validateTableVersionTask = new ValidateTableVersionTask(schemaName, tableVersions);

        DdlTask changeGsiStatusTask;
        if ("VISIBLE".equalsIgnoreCase(visibility)) {
            changeGsiStatusTask = new GsiUpdateIndexVisibilityTask(schemaName,
                primaryTableName,
                indexTableName,
                IndexVisibility.INVISIBLE,
                IndexVisibility.VISIBLE);
        } else {
            changeGsiStatusTask = new GsiUpdateIndexVisibilityTask(schemaName,
                primaryTableName,
                indexTableName,
                IndexVisibility.VISIBLE,
                IndexVisibility.INVISIBLE);
        }

        DdlTask syncTask = new TableSyncTask(schemaName, primaryTableName);

        List<DdlTask> taskList = ImmutableList.of(
            validateTask,
            validateTableVersionTask,
            changeGsiStatusTask,
            syncTask
        );
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);

        return executableDdlJob;
    }

    public static ExecutableDdlJob create(AlterTableWithGsiPreparedData alterTableWithGsiPreparedData,
                                          ExecutionContext executionContext) {
        return new AlterGsiVisibilityJobFactory(alterTableWithGsiPreparedData, executionContext).create();
    }
}
