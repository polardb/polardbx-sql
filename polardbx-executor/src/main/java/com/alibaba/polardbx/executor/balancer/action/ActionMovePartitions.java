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

package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Action that move multiple partitions concurrently
 *
 * @author luoyhanxin
 * @since 2022/03
 */
public class ActionMovePartitions implements BalanceAction, Comparable<ActionMovePartitions> {

    private final String schema;
    //key:tableGroup, val:alter tablegroup move partitions
    private final Map<String, List<ActionMovePartition>> actions;

    public ActionMovePartitions(String schema, Map<String, List<ActionMovePartition>> actions) {
        this.schema = schema;
        this.actions = actions;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return "MovePartitions";
    }

    @Override
    public String getStep() {
        List<ActionMovePartition> actionMovePartitions = new ArrayList<>();
        GeneralUtil.emptyIfNull(actions.entrySet()).stream().forEach(o -> actionMovePartitions.addAll(o.getValue()));
        return StringUtils.join(actionMovePartitions, ",");
    }

    @Override
    public Long getBackfillRows() {
        Long backfillRows = 0L;
        for (String toGroup : actions.keySet()) {
            backfillRows += actions.get(toGroup).stream().map(o -> o.getBackfillRows()).mapToLong(o -> o).sum();
        }
        return backfillRows;
    }

    /*
     * Convert it to concurrent move-partition jobs like:
     *
     * Empty
     * /   \
     * MovePartition | MovePartition | MovePartition
     * \                   /           /
     *  Empty
     *
     * @param ec
     * @return
     */
    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        ExecutableDdlJob job = new ExecutableDdlJob();
        job.setMaxParallelism(ec.getParamManager().getInt(ConnectionParams.REBALANCE_TASK_PARALISM));
        EmptyTask headTask = new EmptyTask(schema);
        job.addTask(headTask);
        job.labelAsHead(headTask);

        Map<String, Pair<Pair<List<DdlTask>, List<DdlTask>>, Set<String>>> tableGroupTaskInfo =
            new TreeMap<>(String::compareToIgnoreCase);
        TableGroupInfoManager tableGroupInfoManager = OptimizerContext.getContext(schema).getTableGroupInfoManager();
        Map<String, Set<String>> tableGroupPrimaryTables = new TreeMap<>(String::compareToIgnoreCase);

        for (Map.Entry<String, List<ActionMovePartition>> entry : actions.entrySet()) {
            ExecutableDdlJob subJob = new ExecutableDdlJob();
            for (ActionMovePartition move : entry.getValue()) {
                ExecutableDdlJob ddlTask = move.toDdlJob(ec);
                subJob.appendJob2(ddlTask);
            }
            Set<String> relatedTableGroup;
            if (!tableGroupTaskInfo.containsKey(entry.getKey())) {
                relatedTableGroup = getRelatedTableGroupNames(entry.getKey(), tableGroupInfoManager, ec);
                tableGroupPrimaryTables.put(entry.getKey(), getPrimaryTables(entry.getKey()));

            } else {
                relatedTableGroup = tableGroupTaskInfo.get(entry.getKey()).getValue();
            }
            job.appendJobAfter2(headTask, subJob);
            addDependencyRelationship(job, tableGroupTaskInfo, subJob, entry.getKey(), relatedTableGroup,
                tableGroupPrimaryTables);

            if (!tableGroupTaskInfo.containsKey(entry.getKey())) {
                List<DdlTask> headNodes = subJob.getAllZeroInDegreeVertexes().stream().map(o -> o.getObject()).collect(
                    Collectors.toList());
                List<DdlTask> tailNodes = subJob.getAllZeroOutDegreeVertexes().stream().map(o -> o.getObject()).collect(
                    Collectors.toList());

                Pair<Pair<List<DdlTask>, List<DdlTask>>, Set<String>> tableGroupInfo =
                    Pair.of(Pair.of(headNodes, tailNodes), relatedTableGroup);
                tableGroupTaskInfo.put(entry.getKey(), tableGroupInfo);
            }
        }

        ExecutableDdlJob tailJob = new ExecutableDdlJob();
        EmptyTask tailTask = new EmptyTask(schema);
        tailJob.addTask(tailTask);
        job.appendJob2(tailJob);

        job.labelAsTail(tailTask);
        boolean removeRedundancyRelation =
            ec.getParamManager().getBoolean(ConnectionParams.REMOVE_DDL_JOB_REDUNDANCY_RELATIONS);
        if (removeRedundancyRelation) {
            job.removeRedundancyRelations();
        }
        return job;
    }

    private Set<String> getPrimaryTables(String tableGroupName) {
        Set<String> primaryTables = new TreeSet<>(String::compareToIgnoreCase);
        TableGroupInfoManager tableGroupInfoManager = OptimizerContext.getContext(schema).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        SchemaManager schemaManager = OptimizerContext.getContext(schema).getLatestSchemaManager();
        for (TablePartRecordInfoContext tableInfo : tableGroupConfig.getAllTables()) {
            TableMeta tableMeta = schemaManager.getTable(tableInfo.getTableName());
            String primaryTableName = tableMeta.getTableName();
            if (tableMeta.isGsi()) {
                assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            }
            primaryTables.add(primaryTableName);
        }
        return primaryTables;
    }

    private Set<String> getRelatedTableGroupNames(String tableGroup, TableGroupInfoManager tableGroupInfoManager,
                                                  ExecutionContext executionContext) {
        Set<String> tableGroups = new TreeSet<>(String::compareToIgnoreCase);
        tableGroups.add(tableGroup);
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroup);
        if (tableGroupConfig != null) {
            for (TablePartRecordInfoContext tablePartCon : GeneralUtil.emptyIfNull(tableGroupConfig.getAllTables())) {
                TableMeta tableMeta = executionContext.getSchemaManager(schema).getTable(tablePartCon.getTableName());
                if (tableMeta.isGsi()) {
                    String primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                    tableMeta = OptimizerContext.getContext(schema).getLatestSchemaManager().getTable(primaryTableName);
                    TableGroupConfig curTableConfig =
                        tableGroupInfoManager.getTableGroupConfigById(tableMeta.getPartitionInfo().getTableGroupId());
                    if (curTableConfig != null) {
                        tableGroups.add(curTableConfig.getTableGroupRecord().getTg_name());
                    }
                }
            }
        }
        return tableGroups;
    }

    /**
     * parentJob: the parent job
     * tableGroupTask: the exists tableGroup subJobs
     * ddlTask: the current tableGroup subJob to be added
     * if there are some dependency between ddlTask and
     * the exists tableGroup subJobs, need to add the relationship
     * for them.
     * relatedTableGroup: the related tableGroups for tables in current tableGroup
     */
    private void addDependencyRelationship(ExecutableDdlJob parentJob,
                                           Map<String, Pair<Pair<List<DdlTask>, List<DdlTask>>, Set<String>>> tableGroupTaskInfo,
                                           ExecutableDdlJob ddlTask, String curTableGroup,
                                           Set<String> relatedTableGroup,
                                           Map<String, Set<String>> tableGroupPrimaryTables) {

        for (Map.Entry<String, Pair<Pair<List<DdlTask>, List<DdlTask>>, Set<String>>> entry : tableGroupTaskInfo.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(curTableGroup)) {
                continue;
            }
            boolean match = entry.getValue().getValue().contains(curTableGroup);
            if (match) {
                for (DirectedAcyclicGraph.Vertex vertex : ddlTask.getAllZeroOutDegreeVertexes()) {
                    for (DdlTask head : entry.getValue().getKey().getKey()) {
                        parentJob.addTaskRelationship(vertex.getObject(), head);
                        if (!parentJob.isValid()) {
                            parentJob.removeTaskRelationship(vertex.getObject(), head);
                        } else {
                            parentJob.removeTaskRelationship(parentJob.getHead(), head);
                        }
                    }
                }
            } else {
                match = relatedTableGroup.contains(entry.getKey());
                if (match) {
                    for (DirectedAcyclicGraph.Vertex vertex : ddlTask.getAllZeroInDegreeVertexes()) {
                        for (DdlTask tail : entry.getValue().getKey().getValue()) {
                            parentJob.addTaskRelationship(tail, vertex.getObject());
                            if (!parentJob.isValid()) {
                                parentJob.removeTaskRelationship(tail, vertex.getObject());
                            } else {
                                parentJob.removeTaskRelationship(parentJob.getHead(), vertex.getObject());
                            }
                        }
                    }
                }
            }
            if (!match) {
                match = entry.getValue().getValue().stream().anyMatch(o -> relatedTableGroup.contains(o));
                if (match) {
                    Set<String> sourcePrimaryTables;
                    Set<String> targetPrimaryTables;
                    if (tableGroupPrimaryTables.containsKey(curTableGroup)) {
                        sourcePrimaryTables = tableGroupPrimaryTables.get(curTableGroup);
                    } else {
                        sourcePrimaryTables = getPrimaryTables(curTableGroup);
                        tableGroupPrimaryTables.put(curTableGroup, sourcePrimaryTables);
                    }
                    if (tableGroupPrimaryTables.containsKey(entry.getKey())) {
                        targetPrimaryTables = tableGroupPrimaryTables.get(entry.getKey());
                    } else {
                        targetPrimaryTables = getPrimaryTables(entry.getKey());
                        tableGroupPrimaryTables.put(entry.getKey(), targetPrimaryTables);
                    }
                    if (GeneralUtil.isNotEmpty(Sets.intersection(sourcePrimaryTables, targetPrimaryTables))) {
                        for (DirectedAcyclicGraph.Vertex vertex : ddlTask.getAllZeroOutDegreeVertexes()) {
                            for (DdlTask head : entry.getValue().getKey().getKey()) {
                                parentJob.addTaskRelationship(vertex.getObject(), head);
                                if (!parentJob.isValid()) {
                                    parentJob.removeTaskRelationship(vertex.getObject(), head);
                                } else {
                                    parentJob.removeTaskRelationship(parentJob.getHead(), head);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionMovePartitions)) {
            return false;
        }
        ActionMovePartitions that = (ActionMovePartitions) o;
        return Objects.equals(schema, that.schema) && Objects.equals(actions, that.actions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, actions);
    }

    @Override
    public String toString() {
        return "ActionMovePartitions{" +
            "schema='" + schema + '\'' +
            ", actions=" + actions +
            '}';
    }

    public Map<String, List<ActionMovePartition>> getActions() {
        return actions;
    }

    @Override
    public int compareTo(ActionMovePartitions o) {
        if (actions.size() == o.actions.size()) {
            for (Map.Entry<String, List<ActionMovePartition>> entry : actions.entrySet()) {
                if (o.getActions().get(entry.getKey()) != null) {
                    for (int i = 0; i < Math.min(entry.getValue().size(), o.actions.get(entry.getKey()).size()); i++) {
                        int res = entry.getValue().get(i).compareTo(o.actions.get(entry.getKey()).get(i));
                        if (res != 0) {
                            return res;
                        }
                    }
                } else {
                    return 1;
                }
            }
        }
        return Integer.compare(actions.size(), o.actions.size());
    }
}
