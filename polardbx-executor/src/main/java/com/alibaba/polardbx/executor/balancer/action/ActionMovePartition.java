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

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.balancer.stats.TableGroupStat;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Move partition to balance data
 *
 * @author moyi
 * @since 2021/03
 */
@Getter
@Setter
public class ActionMovePartition implements BalanceAction, Comparable<ActionMovePartition> {

    private static final Logger LOG = LoggerFactory.getLogger(ActionMovePartition.class);

    public static final String NAME = "MovePartition";
    private static final String MOVE_PARTITION_SQL = "alter tablegroup %s move partitions %s to %s";
    private static final String MOVE_SUBPARTITION_SQL = "alter tablegroup %s move subpartitions %s to %s";
    private static final String MOVE_PARTITIONS_SUBSQL = " (%s) to %s";
    private static final String ALTER_TABLEGROUP_PARTITIONS_SUBSQL = "alter tablegroup %s move partitions ";
    private static final String ALTER_TABLEGROUP_SUBPARTITIONS_SUBSQL = "alter tablegroup %s move subpartitions ";

    private String schema;
    private String tableGroupName;
    private List<String> partitionNames;
    private String toGroup;
    private String toInst;
    private BalanceStats stats;
    private Boolean isSubpartition;

    private ActionMovePartition(String schema) {
        this.schema = schema;
    }

    public ActionMovePartition(String schemaName, String tableGroupName, String partitionName,
                               String toGroup, String toInst) {
        this.schema = schemaName;
        this.tableGroupName = tableGroupName;
        this.partitionNames = Arrays.asList(partitionName);
        this.toGroup = toGroup;
        this.toInst = toInst;
    }

    @Override
    public Long getBackfillRows() {
        Set<String> partitionNameSet = new HashSet<>(this.partitionNames);
        TableGroupStat tableGroupStat = stats.getTableGroupStats().stream()
            .filter(o -> o.getTableGroupConfig().getTableGroupRecord().getTg_name().equals(this.tableGroupName))
            .collect(
                Collectors.toList()).get(0);
        return tableGroupStat.getPartitionGroups().stream()
            .filter(o -> partitionNameSet.contains((o.pg == null) ? "" : o.pg.getPartition_name()))
            .map(
                PartitionGroupStat::getDataRows).mapToLong(o -> o).sum();
    }

    @Override
    public Long getDiskSize() {
        Set<String> partitionNameSet = new HashSet<>(this.partitionNames);
        TableGroupStat tableGroupStat = stats.getTableGroupStats().stream()
            .filter(o -> o.getTableGroupConfig().getTableGroupRecord().getTg_name().equals(this.tableGroupName))
            .collect(
                Collectors.toList()).get(0);
        return tableGroupStat.getPartitionGroups().stream()
            .filter(o -> partitionNameSet.contains((o.pg == null) ? "" : o.pg.getPartition_name()))
            .map(
                PartitionGroupStat::getTotalDiskSize).mapToLong(o -> o).sum();
    }

    @Override
    public double getLogicalTableCount() {
        TableGroupStat tableGroupStat = stats.getTableGroupStats().stream()
            .filter(o -> o.getTableGroupConfig().getTableGroupRecord().getTg_name().equals(this.tableGroupName))
            .collect(
                Collectors.toList()).get(0);

        return tableGroupStat.getTableGroupConfig().getTableCount();
    }

    public static List<ActionMovePartition> createMoveToGroups(String schema,
                                                               List<PartitionStat> partitions,
                                                               String toGroup,
                                                               BalanceStats stats) {
        List<ActionMovePartition> res = new ArrayList<>();

        GeneralUtil.emptyIfNull(partitions).stream()
            .collect(
                Collectors.groupingBy(
                    PartitionStat::getTableGroupName,
                    Collectors.mapping(o -> o, Collectors.toList())))
            .forEach((tableGroupName, partList) -> {
                List<String> partNameList =
                    partList.stream().map(o -> o.getPartitionName()).collect(Collectors.toList());
                Boolean isSubpartition = partList.get(0).isSubPartition();
                res.add(createMoveToGroup(schema, tableGroupName, partNameList, toGroup, stats, isSubpartition));
            });

        return res;
    }

    public static List<ActionMovePartition> createMoveToInsts(String schema,
                                                              List<PartitionStat> partitions,
                                                              String toInst,
                                                              BalanceStats stats) {
        List<ActionMovePartition> res = new ArrayList<>();

        GeneralUtil.emptyIfNull(partitions).stream()
            .collect(
                Collectors.groupingBy(
                    PartitionStat::getTableGroupName,
                    Collectors.mapping(PartitionStat::getPartitionName, Collectors.toList())))
            .forEach((tableGroupName, partList) -> {
                res.add(createMoveToInst(schema, tableGroupName, partList, toInst, stats));
            });

        return res;
    }

    private static ActionMovePartition createMoveToGroup(String schema,
                                                         String tgName,
                                                         List<String> partitions,
                                                         String toGroup,
                                                         BalanceStats stats,
                                                         Boolean isSubpartition) {
        ActionMovePartition res = new ActionMovePartition(schema);
        res.tableGroupName = tgName;
        res.partitionNames = partitions;
        res.toGroup = toGroup;
        res.stats = stats;
        res.isSubpartition = isSubpartition;
        return res;
    }

    private static ActionMovePartition createMoveToInst(String schema,
                                                        String tgName,
                                                        List<String> partitions,
                                                        String toInst,
                                                        BalanceStats stats) {
        ActionMovePartition res = new ActionMovePartition(schema);
        res.tableGroupName = tgName;
        res.partitionNames = partitions;
        res.toInst = toInst;
        res.stats = stats;
        res.isSubpartition = false;
        return res;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getStep() {
        String target = this.toInst != null ?
            "instance(" + this.toInst + ")" :
            "group(" + this.toGroup + ")";
        String stepTemplate = "move partition %s.%s to %s";
        if (isSubpartition) {
            stepTemplate = "move subpartition %s.%s to %s";
        }
        return String.format(stepTemplate,
            this.tableGroupName, TStringUtil.join(this.partitionNames, ","), target);
    }

    public String getSql() {
        String targetStorage = this.toInst != null ?
            this.toInst : DbTopologyManager.getStorageInstIdByGroupName(schema, this.toGroup);
        if (TStringUtil.isBlank(targetStorage)) {
            throw new TddlRuntimeException(ErrorCode.ERR_REBALANCE,
                "target storage not found: group=" + this.toGroup);
        }
        String partitionList =
            this.partitionNames.stream().map(TStringUtil::backQuote).collect(Collectors.joining(","));
        String sql = null;
        if (isSubpartition) {
            sql = String.format(MOVE_SUBPARTITION_SQL,
                TStringUtil.backQuote(this.tableGroupName),
                partitionList,
                TStringUtil.quoteString(targetStorage));

        } else {
            sql = String.format(MOVE_PARTITION_SQL,
                TStringUtil.backQuote(this.tableGroupName),
                partitionList,
                TStringUtil.quoteString(targetStorage));
        }
        return sql;
    }

    public static String getMovesSql(String schema, String tableGroupName, List<ActionMovePartition> moves) {
        Map<String, String> targetStorageMap =
            DbTopologyManager.getGroupNameToStorageInstIdMap(schema);
        List<String> subSqls = new ArrayList<>();
        String sql = String.format(ALTER_TABLEGROUP_PARTITIONS_SUBSQL, TStringUtil.backQuote(tableGroupName));
        if (moves.get(0).isSubpartition) {
            sql = String.format(ALTER_TABLEGROUP_SUBPARTITIONS_SUBSQL, TStringUtil.backQuote(tableGroupName));
        }
        for (ActionMovePartition move : moves) {
            if (!targetStorageMap.containsKey(move.toGroup)) {
                throw new TddlRuntimeException(ErrorCode.ERR_REBALANCE,
                    "target storage not found: group=" + move.toGroup);
            }
            String targetStorage = targetStorageMap.get(move.toGroup);
            String partitionList =
                move.partitionNames.stream().map(TStringUtil::backQuote).collect(Collectors.joining(","));
            String subSql;
            subSql = String.format(MOVE_PARTITIONS_SUBSQL,
                partitionList,
                TStringUtil.quoteString(targetStorage));
            subSqls.add(subSql);
        }
        if (subSqls.size() > 0) {
            sql = sql + StringUtils.join(", ", subSqls) + ";";
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_REBALANCE,
                "move action not found in ActionMovePartitions");
        }
        return sql;
    }

    public static ExecutableDdlJob movesToDdlJob(String tableGroupName, List<ActionMovePartition> moves,
                                                 ExecutionContext ec) {
        long totalRows = 0L;
        long totalSize = 0L;
        if (moves.size() <= 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_REBALANCE,
                "move action not found in ActionMovePartitions");
        }
        ActionMovePartition move = moves.get(0);
        String schema = move.schema;
        String sql = getMovesSql(schema, tableGroupName, moves);
        BalanceStats stats = move.stats;
        List<String> partitionNames = new ArrayList<>();
        moves.stream().forEach(o -> partitionNames.addAll(o.partitionNames));
        try {
            List<PartitionStat> partitionStatList =
                stats.filterPartitionStat(tableGroupName, Sets.newHashSet(partitionNames));
            for (PartitionStat partitionStat : partitionStatList) {
                totalRows += partitionStat.getPartitionRows();
                totalSize += partitionStat.getPartitionDiskSize();
            }
        } catch (Exception e) {
            EventLogger.log(EventType.DDL_WARN, "calculate rebalance rows error. " + e.getMessage());
        }
        return ActionUtils.convertToDelegatorJob(move.schema, sql,
            CostEstimableDdlTask.createCostInfo(totalRows, totalSize, (long) move.getLogicalTableCount()));
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        String sql = getSql();
        long totalRows = 0L;
        long totalSize = 0L;
        try {
            List<PartitionStat> partitionStatList =
                stats.filterPartitionStat(tableGroupName, Sets.newHashSet(partitionNames));
            for (PartitionStat partitionStat : partitionStatList) {
                totalRows += partitionStat.getPartitionRows();
                totalSize += partitionStat.getPartitionDiskSize();
            }
        } catch (Exception e) {
            EventLogger.log(EventType.DDL_WARN, "calculate rebalance rows error. " + e.getMessage());
        }
        return ActionUtils.convertToDelegatorJob(schema, sql,
            CostEstimableDdlTask.createCostInfo(totalRows, totalSize, (long) getLogicalTableCount()));
    }

    @Override
    public String toString() {
        return getStep();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionMovePartition)) {
            return false;
        }
        ActionMovePartition movePartition = (ActionMovePartition) o;
        return Objects.equals(this.schema, movePartition.schema) &&
            Objects.equals(this.tableGroupName, movePartition.tableGroupName) && Objects
            .equals(this.partitionNames, movePartition.partitionNames) &&
            Objects.equals(this.toInst, movePartition.toInst);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, tableGroupName, partitionNames, toInst);
    }

    @Override
    public int compareTo(ActionMovePartition o) {
        int res = schema.compareTo(o.schema);
        if (res != 0) {
            return res;
        }
        res = tableGroupName.compareTo(o.tableGroupName);
        if (res != 0) {
            return res;
        }

        res = toInst.compareTo(o.toInst);
        if (res != 0) {
            return res;
        }
        for (int i = 0; i < Math.min(partitionNames.size(), o.partitionNames.size()); i++) {
            res = partitionNames.get(i).compareTo(o.partitionNames.get(i));
            if (res != 0) {
                return res;
            }
        }
        return Integer.compare(partitionNames.size(), o.partitionNames.size());
    }
}
