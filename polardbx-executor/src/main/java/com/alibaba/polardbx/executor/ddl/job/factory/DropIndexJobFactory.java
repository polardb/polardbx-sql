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
import com.alibaba.polardbx.executor.ddl.job.builder.DropLocalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropIndexHideMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropIndexPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropIndexRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropIndexValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropLocalIndexPreparedData;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DropIndexJobFactory extends DdlJobFactory {

    private final List<PhysicalPlanData> physicalPlanDataList;

    public DropIndexJobFactory(List<PhysicalPlanData> physicalPlanDataList) {
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
            List<DdlTask> tasks = createTasksForOneJob(physicalPlanData);
            taskList.addAll(tasks);
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
        DdlTask validateTask = new DropIndexValidateTask(schemaName, logicalTableName, indexName, tableGroupConfig);
        DdlTask hideMetaTask = new DropIndexHideMetaTask(schemaName, logicalTableName, indexName);
        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);
        DdlTask phyDdlTask = new DropIndexPhyDdlTask(schemaName, physicalPlanData);
        DdlTask cdcMarkDdlTask =
            CBOUtil.isOss(schemaName, logicalTableName) || CBOUtil.isGsi(schemaName, logicalTableName) ? null :
                new CdcDdlMarkTask(schemaName, physicalPlanData, false, false);
        DdlTask removeMetaTask = new DropIndexRemoveMetaTask(schemaName, logicalTableName, indexName);
        TableSyncTask removeMetaSyncTask = new TableSyncTask(schemaName, logicalTableName);

        return Lists.newArrayList(
            validateTask,
            hideMetaTask,
            tableSyncTask,
            phyDdlTask,
            cdcMarkDdlTask,
            removeMetaTask,
            removeMetaSyncTask
        ).stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        for (PhysicalPlanData physicalPlanData : physicalPlanDataList) {
            resources.add(concatWithDot(physicalPlanData.getSchemaName(), physicalPlanData.getLogicalTableName()));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    /**
     * Drop local index, which could be explicit local index, or implicit local index for GSI
     */
    private static PhysicalPlanData buildSingleLocalIndexData(DDL ddl,
                                                              DropLocalIndexPreparedData dropLocalIndexPreparedData,
                                                              boolean implicit,
                                                              SqlNode sqlNode,
                                                              ExecutionContext executionContext) {
        return DropLocalIndexBuilder.create(ddl,
                dropLocalIndexPreparedData,
                executionContext)
            .withImplicitIndex(implicit, sqlNode)
            .build().genPhysicalPlanData();
    }

    /**
     * Drop local index by rewrite sql node
     */
    public static ExecutableDdlJob createDropLocalIndex(DDL ddl,
                                                        SqlNode sqlNode,
                                                        List<DropLocalIndexPreparedData> preparedDataList,
                                                        boolean implicit,
                                                        ExecutionContext ec) {
        if (CommonUtils.isEmpty(preparedDataList)) {
            return null;
        }
        List<PhysicalPlanData> physicalPlanDataList =
            preparedDataList.stream()
                .map(x -> buildSingleLocalIndexData(ddl, x, implicit, sqlNode, ec))
                .collect(Collectors.toList());
        return new DropIndexJobFactory(physicalPlanDataList).create();
    }

}
