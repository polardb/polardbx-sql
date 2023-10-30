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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupTruncatePartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TruncateTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupTruncatePartitionPreparedData;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class AlterTableGroupTruncatePartitionJobFactory extends DdlJobFactory {

    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;

    protected DDL ddl;
    protected AlterTableGroupTruncatePartitionPreparedData preparedData;
    protected ExecutionContext executionContext;

    public AlterTableGroupTruncatePartitionJobFactory(DDL ddl,
                                                      AlterTableGroupTruncatePartitionPreparedData preparedData,
                                                      ExecutionContext executionContext) {
        this.ddl = ddl;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String schemaName = preparedData.getSchemaName();
        String tableGroupName = preparedData.getTableGroupName();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        AlterTableGroupTruncatePartitionBuilder builder = getDdlPhyPlanBuilder();

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);

        Map<String, Long> tableVersions = getTableVersions(tableGroupConfig);

        boolean isBrdTg = tableGroupConfig.getTableGroupRecord().isBroadCastTableGroup();
        DdlTask validateTask = new AlterTableGroupValidateTask(schemaName, tableGroupName, tableVersions, true,
            isBrdTg ? null : preparedData.getTargetPhysicalGroups());

        executableDdlJob.labelAsHead(validateTask);

        constructSubTasks(schemaName, executableDdlJob, validateTask, builder);

        executableDdlJob.setMaxParallelism(ScaleOutUtils.getTableGroupTaskParallelism(executionContext));
        return executableDdlJob;
    }

    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  AlterTableGroupTruncatePartitionBuilder builder) {
        for (Map.Entry<String, Long> entry : builder.getTableVersions().entrySet()) {
            String tableName = entry.getKey();
            Long tableVersion = entry.getValue();

            PhysicalPlanData physicalPlanData = builder.getPhyPlanDataMap().get(tableName);
            physicalPlanData.setTruncatePartition(true);

            ExecutableDdlJob subTasks = buildSubTasks(schemaName, tableName, tableVersion, physicalPlanData);

            executableDdlJob.combineTasks(subTasks);
            executableDdlJob.addTaskRelationship(tailTask, subTasks.getHead());

            executableDdlJob.getExcludeResources().addAll(subTasks.getExcludeResources());
        }
    }

    private ExecutableDdlJob buildSubTasks(String schemaName, String tableName,
                                           Long tableVersion, PhysicalPlanData physicalPlanData) {
        ExecutableDdlJob subTasks = new ExecutableDdlJob();

        Map<String, Long> tableVersions = new HashMap<>(1);
        tableVersions.put(tableName, tableVersion);
        ValidateTableVersionTask validateTableVersionTask = new ValidateTableVersionTask(schemaName, tableVersions);

        DdlTask phyDdlTask = new TruncateTablePhyDdlTask(schemaName, physicalPlanData);
        DdlTask cdcDdlMarkTask = new CdcDdlMarkTask(schemaName, physicalPlanData, false, false);

        subTasks.addSequentialTasks(Lists.newArrayList(
            validateTableVersionTask,
            phyDdlTask,
            cdcDdlMarkTask
        ));

        subTasks.labelAsHead(validateTableVersionTask);

        return subTasks;
    }

    protected Map<String, Long> getTableVersions(TableGroupConfig tableGroupConfig) {
        String schemaName = preparedData.getSchemaName();
        Map<String, Long> tableVersions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        tableGroupConfig.getAllTables().forEach(t -> {
            String tableName = t.getTableName();
            Long tableVersion = executionContext.getSchemaManager(schemaName).getTable(tableName).getVersion();
            tableVersions.put(tableName, tableVersion);
        });

        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            TablesAccessor tablesAccessor = new TablesAccessor();
            tablesAccessor.setConnection(conn);
            for (Map.Entry<String, Long> entry : tableVersions.entrySet()) {
                String tableName = entry.getKey();
                Long tableVersion = entry.getValue();

                TablesRecord tablesRecord = tablesAccessor.query(schemaName, tableName, false);

                LOG.warn(String.format("%s current tableVersion in Ec:%d", tableName, tableVersion));

                if (tablesRecord != null) {
                    LOG.warn(String.format("current tablesRecord details in prepare phase: %s", tablesRecord));
                } else {
                    LOG.warn(
                        String.format("current tablesRecord details: %s.%s %s", schemaName, tableName, " not exists"));
                }
            }
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }

        return tableVersions;
    }

    protected AlterTableGroupTruncatePartitionBuilder getDdlPhyPlanBuilder() {
        return (AlterTableGroupTruncatePartitionBuilder) new AlterTableGroupTruncatePartitionBuilder(ddl, preparedData,
            executionContext).build();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
        for (String partitionName : preparedData.getTruncatePartitionNames()) {
            resources.add(concatWithDot(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()),
                partitionName));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}
