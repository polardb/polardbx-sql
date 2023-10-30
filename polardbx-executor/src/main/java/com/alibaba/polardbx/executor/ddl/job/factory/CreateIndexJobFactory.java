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

import com.alibaba.polardbx.common.CommonUtils;
import com.alibaba.polardbx.executor.ddl.job.builder.CreateLocalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateIndexAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateIndexPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateIndexShowMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateIndexValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class CreateIndexJobFactory extends DdlJobFactory {

    private final List<PhysicalPlanData> physicalPlanDataList;

    public CreateIndexJobFactory(List<PhysicalPlanData> physicalPlanDataList) {
        this.physicalPlanDataList = physicalPlanDataList;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        if (physicalPlanDataList.isEmpty()) {
            return executableDdlJob;
        }

        List<DdlTask> taskList = new ArrayList<>();
        for (PhysicalPlanData physicalPlanData : physicalPlanDataList) {
            taskList.addAll(createTasksForOneJob(physicalPlanData));
        }
        if (taskList.isEmpty()) {
            return executableDdlJob;
        }
        executableDdlJob.addSequentialTasks(taskList);
        executableDdlJob.labelAsHead(taskList.get(0));
        executableDdlJob.labelAsTail(taskList.get(taskList.size() - 1));

        return executableDdlJob;
    }

    private List<DdlTask> createTasksForOneJob(PhysicalPlanData physicalPlanData) {
        String schemaName = physicalPlanData.getSchemaName();
        String logicalTableName = physicalPlanData.getLogicalTableName();
        String indexName = physicalPlanData.getIndexName();

        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        TableGroupConfig tableGroupConfig = isNewPart ? physicalPlanData.getTableGroupConfig() : null;
        DdlTask validateTask = new CreateIndexValidateTask(schemaName, logicalTableName, indexName, tableGroupConfig);
        DdlTask addMetaTask = new CreateIndexAddMetaTask(schemaName, logicalTableName);
        DdlTask phyDdlTask =
            new CreateIndexPhyDdlTask(schemaName, physicalPlanData).onExceptionTryRecoveryThenRollback();
        DdlTask cdcDdlMarkTask =
            CBOUtil.isOss(schemaName, logicalTableName) || CBOUtil.isGsi(schemaName, logicalTableName) ? null :
                new CdcDdlMarkTask(schemaName, physicalPlanData, false, false);
        DdlTask showMetaTask = new CreateIndexShowMetaTask(schemaName, logicalTableName, indexName,
            physicalPlanData.getDefaultDbIndex(), physicalPlanData.getDefaultPhyTableName());
        DdlTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);

        return Lists.newArrayList(
            validateTask,
            addMetaTask,
            phyDdlTask,
            cdcDdlMarkTask,
            showMetaTask,
            tableSyncTask
        ).stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        for (PhysicalPlanData physicalPlanData : physicalPlanDataList) {
            resources.add(concatWithDot(physicalPlanData.getSchemaName(), physicalPlanData.getLogicalTableName()));
        }
    }

    /**
     * Build physical plan for local index, which could be either explicit or implicit.
     * Implicit local index will be prefixed with `_local_`.
     */
    private static PhysicalPlanData buildLocalIndexData(DDL ddl,
                                                        CreateLocalIndexPreparedData createLocalIndexPreparedData,
                                                        boolean implicit,
                                                        SqlNode sqlNode,
                                                        ExecutionContext executionContext) {
        return CreateLocalIndexBuilder.create(
            ddl,
            createLocalIndexPreparedData,
            executionContext
        ).withImplicitIndex(implicit, sqlNode).build().genPhysicalPlanData();
    }

    /**
     * Build a job to create local index by rewrite sql-node
     */
    public static ExecutableDdlJob createLocalIndex(DDL ddl,
                                                    SqlNode sqlNode,
                                                    List<CreateLocalIndexPreparedData> localIndexPreparedData,
                                                    ExecutionContext ec) {
        if (CommonUtils.isEmpty(localIndexPreparedData)) {
            return null;
        }
        List<PhysicalPlanData> localIndexData =
            localIndexPreparedData
                .stream()
                .map(x -> buildLocalIndexData(ddl, x, x.isOnGsi(), sqlNode, ec))
                .collect(Collectors.toList());

        return new CreateIndexJobFactory(localIndexData).create();
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
