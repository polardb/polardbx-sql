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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.balancer.stats.TableGroupStat;
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
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.aliyun.oss.common.utils.CaseInsensitiveMap;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlSavepoint;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
    private final List<Pair<String, List<ActionMovePartition>>> actions;

    public ActionMovePartitions(String schema, List<Pair<String, List<ActionMovePartition>>> actions) {
        this.schema = schema;
        this.actions = actions;
    }

    public ActionMovePartitions(String schema, Map<String, List<ActionMovePartition>> actions) {
        this.schema = schema;
        this.actions = actions.entrySet().stream()
            .map(o -> Pair.of(o.getKey(), o.getValue())).collect(Collectors.toList());
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
        GeneralUtil.emptyIfNull(actions).stream().forEach(o -> actionMovePartitions.addAll(o.getValue()));
        return StringUtils.join(actionMovePartitions, ",");
    }

    @Override
    public Long getBackfillRows() {
        Long backfillRows = 0L;
        for (Pair<String, List<ActionMovePartition>> toGroupActions : actions) {
            backfillRows += toGroupActions.getValue().stream().map(o -> o.getBackfillRows()).mapToLong(o -> o).sum();
        }
        return backfillRows;
    }

    @Override
    public Long getDiskSize() {
        Long diskSize = 0L;
        for (Pair<String, List<ActionMovePartition>> toGroupActions : actions) {
            diskSize += toGroupActions.getValue().stream().map(o -> o.getDiskSize()).mapToLong(o -> o).sum();
        }
        return diskSize;
    }

    @Override
    public double getLogicalTableCount() {
        double tableCount = 0;
        for (Pair<String, List<ActionMovePartition>> toGroupActions : actions) {
            tableCount += toGroupActions.getValue().get(0).getLogicalTableCount();
        }

        return Math.max(1.0, tableCount / actions.size());
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
        EmptyTask tailTask = new EmptyTask(schema);
        job.addTask(headTask);
        job.labelAsHead(headTask);
        job.addTask(tailTask);
        job.labelAsTail(tailTask);

        Map<String, List<List<ActionMovePartition>>> actionTableGroups = new CaseInsensitiveMap<>();
        for (Pair<String, List<ActionMovePartition>> toGroupActions : actions) {
            actionTableGroups.computeIfAbsent(toGroupActions.getKey(), k -> new ArrayList<>())
                .add(toGroupActions.getValue());
        }

        TreeSet<String> tableGroupNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        tableGroupNames.addAll(actionTableGroups.keySet());
        Map<String, Set<String>> relatedTableGroupMap = buildRelatedTableGroupMap(ec, schema);
        String dumpRelatedTableGroupMap = JSON.toJSONString(relatedTableGroupMap);
        SQLRecorderLogger.ddlLogger.warn(
            "related table group map in schema " + schema + " :" + dumpRelatedTableGroupMap);
        List<List<String>> sequentialTableGroupNames = buildSequentialTableGroup(tableGroupNames, relatedTableGroupMap);
        String dumpSequentialTableGroupNames = JSON.toJSONString(sequentialTableGroupNames);
        SQLRecorderLogger.ddlLogger.warn(
            "sequential table group names in schema " + schema + " :" + dumpSequentialTableGroupNames);
        String comparedDumpRelatedTableGroupMap = JSON.toJSONString(relatedTableGroupMap);
        Assert.assertTrue(comparedDumpRelatedTableGroupMap.equals(dumpRelatedTableGroupMap));

        Map<String, List<ExecutableDdlJob>> tableGroupSubJobMap = new HashMap<>();

        for (String tableGroup : actionTableGroups.keySet()) {
            List<List<ActionMovePartition>> actions = actionTableGroups.get(tableGroup);
            List<ExecutableDdlJob> subJobs = new ArrayList<>();
            for (List<ActionMovePartition> action : actions) {
                subJobs.add(convertToSubJob(tableGroup, action, ec));
            }
            for (ExecutableDdlJob subJob : subJobs) {
                job.appendJobAfterHead(headTask, subJob);
            }
            tableGroupSubJobMap.put(tableGroup, subJobs);
        }

        Set<String> allTableGroups = new TreeSet<>(actionTableGroups.keySet());
        for (List<String> tableGroups : sequentialTableGroupNames) {
            for (String tableGroup : tableGroups) {
                if (!actionTableGroups.containsKey(tableGroup)) {
                    continue;
                }
                for (String relatedTableGroup : relatedTableGroupMap.get(tableGroup)) {
                    if (allTableGroups.contains(relatedTableGroup)) {
                        appendDependencyForAllSubJob(job, tableGroupSubJobMap.get(tableGroup),
                            tableGroupSubJobMap.get(relatedTableGroup));
                    }
                }
                allTableGroups.remove(tableGroup);
            }
        }

        for (String tableGroup : sequentialTableGroupNames.get(sequentialTableGroupNames.size() - 1)) {
            for (ExecutableDdlJob subJob : tableGroupSubJobMap.get(tableGroup)) {
                job.addTaskRelationship(subJob.getTail(), tailTask);
            }
        }

        job.removeRedundancyRelations();
        return job;
    }

    private ExecutableDdlJob convertToSubJob(String tableGroupName, List<ActionMovePartition> moves,
                                             ExecutionContext ec) {
        ExecutableDdlJob subJob = new ExecutableDdlJob();
        if (ec.getParamManager().getBoolean(ConnectionParams.SCALE_OUT_GENERATE_MULTIPLE_TO_GROUP_JOB)) {
            ExecutableDdlJob ddlTask = ActionMovePartition.movesToDdlJob(tableGroupName, moves, ec);
            subJob.appendJob2(ddlTask);
            subJob.labelAsHead(ddlTask.getHead());
            subJob.labelAsTail(ddlTask.getTail());
        } else {
            Boolean first = true, last = false;
            for (int i = 0; i < moves.size(); i++) {
                ActionMovePartition move = moves.get(i);
                ExecutableDdlJob ddlTask = move.toDdlJob(ec);
                subJob.appendJob2(ddlTask);
                if (i == 0) {
                    subJob.labelAsHead(ddlTask.getHead());
                }
                if (i == moves.size() - 1) {
                    subJob.labelAsTail(ddlTask.getTail());
                }
            }
        }
        return subJob;
    }

    private List<List<String>> buildSequentialTableGroup(TreeSet<String> tableGroupNames,
                                                         final Map<String, Set<String>> relatedTableGroupMap) {
        // full scan
        List<List<String>> sequentialTableGroupNames = new ArrayList<>();
        TreeSet<String> residueTableGroupNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        while (!CollectionUtils.isEmpty(tableGroupNames)) {
            List<String> firstTableGroups = new ArrayList<>();
            while (!CollectionUtils.isEmpty(tableGroupNames)) {
                String tableGroupName = tableGroupNames.pollFirst();
                firstTableGroups.add(tableGroupName);
                Set<String> relatedTableGroupNames = relatedTableGroupMap.get(tableGroupName);
                if (relatedTableGroupNames != null && !relatedTableGroupNames.isEmpty()) {
                    final Set<String> finalTableGroupNames = tableGroupNames;
                    // TODO(residue => related)
                    relatedTableGroupNames =
                        residueTableGroupNames.stream().filter(o -> finalTableGroupNames.contains(o)).collect(
                            Collectors.toSet());
                    tableGroupNames.removeAll(relatedTableGroupNames);
                    residueTableGroupNames.addAll(relatedTableGroupNames);
                }
            }
            sequentialTableGroupNames.add(firstTableGroups);
            tableGroupNames = residueTableGroupNames;
            residueTableGroupNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        }
        return sequentialTableGroupNames;
    }

    private Map<String, Set<String>> buildRelatedTableGroupMap(ExecutionContext ec, String schema) {
        Map<String, Set<String>> relatedTableGroupMap = new CaseInsensitiveMap<>();
        TableGroupInfoManager tableGroupInfoManager = OptimizerContext.getContext(schema).getTableGroupInfoManager();
        SchemaManager schemaManager = OptimizerContext.getContext(schema).getLatestSchemaManager();
        Map<String, List<String>> primaryTable2gsiMap = new CaseInsensitiveMap<>();
        Map<String, String> table2TableGroupMap = new CaseInsensitiveMap<>();
        for (TableMeta tableMeta : schemaManager.getAllTables()) {
            String gsiName = tableMeta.getTableName().toLowerCase();
            if (tableMeta.isGsi()) {
                assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                String primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName.toLowerCase();
                primaryTable2gsiMap.computeIfAbsent(primaryTableName, k -> new ArrayList<>()).add(gsiName);
            }
        }
        List<String> tableGroups = new ArrayList<>();
        tableGroupInfoManager.getTableGroupConfigInfoCache().values()
            .forEach(tableGroupConfigInfo -> tableGroups.add(
                tableGroupConfigInfo.getTableGroupRecord().getTg_name().toLowerCase()));
        for (String tableGroup : tableGroups) {
            TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroup);
            for (String tableName : tableGroupConfig.getAllTables()) {
                table2TableGroupMap.put(tableName.toLowerCase(), tableGroup);
            }
        }

        SQLRecorderLogger.ddlLogger.warn(
            "table2tableGroup map in schema " + schema + " :" + JSON.toJSONString(table2TableGroupMap));
        SQLRecorderLogger.ddlLogger.warn(
            "table2gsi map in schema " + schema + " :" + JSON.toJSONString(primaryTable2gsiMap));
        for (TableMeta tableMeta : schemaManager.getAllTables()) {
            String primaryTableName = tableMeta.getTableName().toLowerCase();
            if (!tableMeta.isGsi()) {
                String tableGroupName = table2TableGroupMap.get(primaryTableName);
                if (tableGroupName != null) {
                    tableGroupName = tableGroupName.toLowerCase();
                    Set<String> relatedTableGroups =
                        primaryTable2gsiMap.getOrDefault(primaryTableName, new ArrayList<>()).stream()
                            .map(o -> table2TableGroupMap.get(o)).collect(
                                Collectors.toSet());
                    relatedTableGroups.remove(tableGroupName);
                    relatedTableGroupMap.computeIfAbsent(tableGroupName,
                            k -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
                        .addAll(relatedTableGroups);
                    for (String relatedTableGroup : relatedTableGroups) {
                        if (!relatedTableGroup.equalsIgnoreCase(tableGroupName)) {
                            relatedTableGroupMap.computeIfAbsent(relatedTableGroup,
                                    k -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
                                .add(tableGroupName);
                            relatedTableGroupMap.computeIfAbsent(relatedTableGroup,
                                    k -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER))
                                .addAll(relatedTableGroups);
                            relatedTableGroupMap.get(relatedTableGroup).remove(relatedTableGroup);
                        }
                    }
                }
            }
        }
        return relatedTableGroupMap;
    }

    private void appendDependencyForAllSubJob(ExecutableDdlJob parentJob,
                                              List<ExecutableDdlJob> beforeSubjobs,
                                              List<ExecutableDdlJob> afterSubjobs) {
        if (beforeSubjobs != null && afterSubjobs != null) {
            for (ExecutableDdlJob beforeSubjob : beforeSubjobs) {
                for (ExecutableDdlJob afterSubjob : afterSubjobs) {
                    parentJob.addTaskRelationship(beforeSubjob.getTail(), afterSubjob.getHead());
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

    public List<Pair<String, List<ActionMovePartition>>> getActions() {
        return actions;
    }

    @Override
    public int compareTo(ActionMovePartitions o) {
        if (actions.size() == o.actions.size()) {
            for (int i = 0; i < actions.size(); i++) {
                Pair<String, List<ActionMovePartition>> entry = actions.get(i);
                if (o.getActions().get(i) != null) {
                    for (int j = 0; j < Math.min(entry.getValue().size(), o.actions.get(j).getValue().size()); j++) {
                        int res = entry.getValue().get(i).compareTo(o.actions.get(j).getValue().get(j));
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
