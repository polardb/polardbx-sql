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

package com.alibaba.polardbx.executor.balancer.solver;

import com.alibaba.polardbx.executor.balancer.splitpartition.SplitPointUtils;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class PartitionSplitInfo {
    public Map<Integer, PartitionGroupStat> partitionGroupStatMap;
    public Map<Integer, Pair<List<SearchDatumInfo>, List<Double>>> splitPointsMap;

    public List<Integer> errorPartitions;

    MixedModel.SolveLevel solveLevel = MixedModel.SolveLevel.NON_HOT_SPLIT;

    public PartitionSplitInfo(Map<Integer, PartitionGroupStat> partitionGroupStatMap,
                              MixedModel.SolveLevel solveLevel) {
        this.partitionGroupStatMap = partitionGroupStatMap;
        this.splitPointsMap = new HashMap<>();
        this.errorPartitions = new ArrayList<>();
        this.solveLevel = solveLevel;
    }

    private Boolean withCloseValue(double a, double b) {
        if (b > a) {
            return withCloseValue(b, a);
        } else {
            return Math.abs((a - b) / a) < 0.3;
        }
    }

    public Boolean checkSplitSizeValid(List<Pair<Integer, List<Double>>> splitPartitions) {
        for (int i = 0; i < splitPartitions.size(); i++) {
            int partitionIndex = splitPartitions.get(i).getKey();
            if (splitPointsMap.containsKey(partitionIndex)) {
                if (splitPointsMap.get(partitionIndex).getValue().size()
                    < splitPartitions.get(i).getValue().size() - 1) {
                    return false;
                }
                double partSum = 0;
                for (int j = 0; j < splitPartitions.get(i).getValue().size() - 1; j++) {
                    partSum += splitPartitions.get(i).getValue().get(j);
                    if (!withCloseValue(partSum, splitPointsMap.get(partitionIndex).getValue().get(j))) {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }
        return true;
    }

    public Boolean checkSplitNumValid(List<SolverUtils.ToSplitPartition> splitPartitions) {
        for (int i = 0; i < splitPartitions.size(); i++) {
            int partitionIndex = splitPartitions.get(i).getIndex();
            if (splitPointsMap.containsKey(partitionIndex)) {
                if (splitPointsMap.get(partitionIndex).getValue().size()
                    < splitPartitions.get(i).getSplitSizes().size() - 1) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    public void reportErrorPartitions(List<SolverUtils.ToSplitPartition> splitPartitions) {
        for (int i = 0; i < splitPartitions.size(); i++) {
            int partitionIndex = splitPartitions.get(i).getIndex();
            if (splitPointsMap.containsKey(partitionIndex)) {
                if (splitPointsMap.get(partitionIndex).getValue().size()
                    < splitPartitions.get(i).getSplitSizes().size() - 1) {
                    errorPartitions.add(partitionIndex);
                    continue;
                }
                double partSum = 0;
                for (int j = 0; j < splitPartitions.get(i).getSplitSizes().size() - 1; j++) {
                    partSum += splitPartitions.get(i).getSplitSizes().get(j);
                    if (!withCloseValue(partSum, splitPointsMap.get(partitionIndex).getValue().get(j))) {
                        errorPartitions.add(partitionIndex);
                        break;
                    }
                }
            } else {
                errorPartitions.add(partitionIndex);
                continue;
            }
        }
    }

    public PartitionStat chooseMinKeyPartition(List<PartitionStat> partitionStats) {
        PartitionStat partitionStat = null;
        if (partitionStats != null && partitionStats.size() > 0) {
            int minPartKeyNum = 1 << 31 - 1;
            long maxSize = -1L;
            for (int i = 0; i < partitionStats.size(); i++) {
                int partKeyNum = partitionStats.get(i).getPartitionBy().getPartitionColumnNameList().size();
                long size = partitionStats.get(i).getPartitionDiskSize();
                if (partKeyNum == minPartKeyNum) {
                    if (size > maxSize) {
                        maxSize = size;
                        partitionStat = partitionStats.get(i);
                    }
                } else if (partKeyNum < minPartKeyNum) {
                    maxSize = size;
                    minPartKeyNum = partKeyNum;
                    partitionStat = partitionStats.get(i);
                }
            }
        }
        return partitionStat;
    }

    public Boolean generateSplitPoints(List<SolverUtils.ToSplitPartition> toSplitPartitions) {

        Boolean nonHotSplit = (solveLevel != MixedModel.SolveLevel.HOT_SPLIT);
        if (splitPointsMap.size() > 0) {
            splitPointsMap.clear();
        }
        for (int i = 0; i < toSplitPartitions.size(); i++) {
            int partitionIndex = toSplitPartitions.get(i).getIndex();
            String tableSchema = partitionGroupStatMap.get(partitionIndex).getFirstPartition().getSchema();
            List<PartitionStat> partitionStats = partitionGroupStatMap.get(partitionIndex).partitions;
            PartitionStat partitionStat = chooseMinKeyPartition(partitionStats);
            if (partitionStat == null) {
                continue;
            }
            String logicalTableName = partitionStat.getPartitionInfo().getTableName();
            String partName = partitionStat.getPartitionName();
            com.alibaba.polardbx.common.utils.Pair<List<SearchDatumInfo>, List<Double>> splitPoints =
                SplitPointUtils.generateSplitBounds(tableSchema, logicalTableName, partName,
                    toSplitPartitions.get(i).getSplitSizes(), nonHotSplit);
            if (splitPoints == null || splitPoints.getKey().isEmpty()) {
                continue;
            }
            splitPointsMap.put(partitionIndex, Pair.of(splitPoints.getKey(), splitPoints.getValue()));
        }
        return true;
    }

}
