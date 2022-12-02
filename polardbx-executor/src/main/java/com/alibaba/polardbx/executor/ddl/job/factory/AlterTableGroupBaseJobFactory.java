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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author luoyanxin
 */
public abstract class AlterTableGroupBaseJobFactory extends DdlJobFactory {

    @Deprecated
    protected final DDL ddl;
    protected final AlterTableGroupBasePreparedData preparedData;
    protected final Map<String, AlterTableGroupItemPreparedData> tablesPrepareData;
    protected final Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap;
    protected final Map<String, Map<String, List<List<String>>>> tablesTopologyMap;
    protected final Map<String, Map<String, Set<String>>> targetTablesTopology;
    protected final Map<String, Map<String, Set<String>>> sourceTablesTopology;
    protected final Map<String, List<Pair<String, String>>> orderedTargetTablesLocations;
    protected final ExecutionContext executionContext;
    protected final ComplexTaskMetaManager.ComplexTaskType taskType;
    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;

    public AlterTableGroupBaseJobFactory(DDL ddl, AlterTableGroupBasePreparedData preparedData,
                                         Map<String, AlterTableGroupItemPreparedData> tablesPrepareData,
                                         Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap,
                                         Map<String, Map<String, List<List<String>>>> tablesTopologyMap,
                                         Map<String, Map<String, Set<String>>> targetTablesTopology,
                                         Map<String, Map<String, Set<String>>> sourceTablesTopology,
                                         Map<String, List<Pair<String, String>>> orderedTargetTablesLocations,
                                         ComplexTaskMetaManager.ComplexTaskType taskType,
                                         ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.tablesPrepareData = tablesPrepareData;
        this.ddl = ddl;
        this.tablesTopologyMap = tablesTopologyMap;
        this.targetTablesTopology = targetTablesTopology;
        this.sourceTablesTopology = sourceTablesTopology;
        this.newPartitionsPhysicalPlansMap = newPartitionsPhysicalPlansMap;
        this.orderedTargetTablesLocations = orderedTargetTablesLocations;
        this.taskType = taskType;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

    }

    public void constructSubTasks(String schemaName, ExecutableDdlJob executableDdlJob, DdlTask tailTask,
                                  List<DdlTask> bringUpAlterTableGroupTasks, String targetPartitionName) {
        EmptyTask  emptyTask = new EmptyTask(schemaName);
        boolean emptyTaskAdded = false;
        for (Map.Entry<String, Map<String, List<List<String>>>> entry : tablesTopologyMap.entrySet()) {
            AlterTableGroupSubTaskJobFactory subTaskJobFactory =
                new AlterTableGroupSubTaskJobFactory(ddl, tablesPrepareData.get(entry.getKey()),
                    newPartitionsPhysicalPlansMap.get(entry.getKey()), tablesTopologyMap.get(entry.getKey()),
                    targetTablesTopology.get(entry.getKey()), sourceTablesTopology.get(entry.getKey()),
                    orderedTargetTablesLocations.get(entry.getKey()), targetPartitionName, false, taskType,
                    executionContext);
            ExecutableDdlJob subTask = subTaskJobFactory.create();
            executableDdlJob.combineTasks(subTask);
            executableDdlJob.addTaskRelationship(tailTask, subTask.getHead());

            if (subTaskJobFactory.getCdcTableGroupDdlMarkTask() != null) {
                if (!emptyTaskAdded) {
                    executableDdlJob.addTask(emptyTask);
                    emptyTaskAdded = true;
                }
                executableDdlJob.addTask(subTaskJobFactory.getCdcTableGroupDdlMarkTask());
                executableDdlJob.addTaskRelationship(subTask.getTail(), emptyTask);
                executableDdlJob.addTaskRelationship(emptyTask, subTaskJobFactory.getCdcTableGroupDdlMarkTask());
                executableDdlJob.addTaskRelationship(subTaskJobFactory.getCdcTableGroupDdlMarkTask(), bringUpAlterTableGroupTasks.get(0));
            } else {
                executableDdlJob.addTaskRelationship(subTask.getTail(), bringUpAlterTableGroupTasks.get(0));
            }

            DdlTask dropUselessTableTask = ComplexTaskFactory
                .CreateDropUselessPhyTableTask(schemaName, entry.getKey(), sourceTablesTopology.get(entry.getKey()),
                    executionContext);
            executableDdlJob.addTask(dropUselessTableTask);
            executableDdlJob.labelAsTail(dropUselessTableTask);
            executableDdlJob
                .addTaskRelationship(bringUpAlterTableGroupTasks.get(bringUpAlterTableGroupTasks.size() - 1),
                    dropUselessTableTask);
            executableDdlJob.getExcludeResources().addAll(subTask.getExcludeResources());
        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()));
        if (StringUtils.isNotEmpty(preparedData.getTargetTableGroup())) {
            resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTargetTableGroup()));
        }
        for (String relatedPart : preparedData.getRelatedPartitions()) {
            resources.add(concatWithDot(concatWithDot(preparedData.getSchemaName(), preparedData.getTableGroupName()),
                relatedPart));
        }

    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    protected Map<String, Long> getTablesVersion() {
        Map<String, Long> tablesVersion = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            TablesAccessor tablesAccessor = new TablesAccessor();
            tablesAccessor.setConnection(conn);
            for (AlterTableGroupItemPreparedData itemPreparedData : tablesPrepareData.values()) {
                tablesVersion.putIfAbsent(itemPreparedData.getPrimaryTableName(), itemPreparedData.getTableVersion());
                TablesRecord tablesRecord =
                    tablesAccessor.query(preparedData.getSchemaName(), itemPreparedData.getPrimaryTableName(), false);
                LOG.warn(
                    String.format("%s current tableVersion in Ec:%d", itemPreparedData.getPrimaryTableName(),
                        itemPreparedData.getTableVersion()));
                if (tablesRecord != null) {
                    LOG.warn(
                        String.format("current tablesRecord details in prepare phase: %s", tablesRecord.toString()));
                } else {
                    LOG.warn(String.format("current tablesRecord details: %s.%s %s", preparedData.getSchemaName(),
                        itemPreparedData.getPrimaryTableName(), " not exists"));
                }
            }
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }

        return tablesVersion;
    }
}
