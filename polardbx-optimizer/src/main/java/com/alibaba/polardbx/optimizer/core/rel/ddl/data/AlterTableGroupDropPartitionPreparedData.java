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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class AlterTableGroupDropPartitionPreparedData extends AlterTableGroupBasePreparedData {

    private static final String ROOT_PART_NAME = "";

    private Map<String, List<String>> oldPartitionNamesByLevel;

    public AlterTableGroupDropPartitionPreparedData() {
    }

    public void prepareInvisiblePartitionGroup(boolean isSubPartition) {
        List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
            .getTableGroupConfigByName(getTableGroupName());
        assert tableGroupConfig != null && GeneralUtil.isNotEmpty(tableGroupConfig.getAllTables());

        String firstTbName = tableGroupConfig.getAllTables().get(0);
        PartitionInfo partInfo =
            OptimizerContext.getContext(getSchemaName()).getPartitionInfoManager().getPartitionInfo(firstTbName);

        PartitionByDefinition partByDef = partInfo.getPartitionBy();
        PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

        Long tableGroupId = partInfo.getTableGroupId();

        Map<String, List<Long>> partPosList =
            getTheNeighborPartitionsOfDelPartitions(partByDef, subPartByDef, isSubPartition);

        if (GeneralUtil.isNotEmpty(partPosList)) {
            int targetDbCount = getTargetGroupDetailInfoExRecords().size();
            int i = 0;
            for (Map.Entry<String, List<Long>> entry : partPosList.entrySet()) {
                String partName = entry.getKey();
                List<Long> positions = entry.getValue();

                if (GeneralUtil.isEmpty(positions)) {
                    continue;
                }

                PartitionSpec partSpec = partByDef.getPartitionByPartName(partName);

                if (TStringUtil.equalsIgnoreCase(partName, ROOT_PART_NAME)) {
                    Set<String> oldPartNames = oldPartitionNamesByLevel.keySet();
                    for (Long pos : positions) {
                        partSpec = partByDef.getPartitions().get(pos.intValue());
                        if (oldPartNames == null || !oldPartNames.contains(partSpec.getName())) {
                            PartitionGroupRecord partGroupRecord =
                                buildPartitionGroupRecord(partSpec.getName(), tableGroupId, targetDbCount, i);
                            inVisiblePartitionGroups.add(partGroupRecord);
                            i++;
                        }
                    }
                } else {
                    List<String> oldSubPartNameList = oldPartitionNamesByLevel.get(partName);

                    Set<String> oldSubPartNames = null;
                    if (oldSubPartNameList != null) {
                        oldSubPartNames = new TreeSet<>(String::compareToIgnoreCase);
                        oldSubPartNames.addAll(oldSubPartNameList);
                    }

                    for (Long pos : positions) {
                        PartitionSpec subPartSpec = partSpec.getSubPartitions().get(pos.intValue());
                        if (oldSubPartNames == null || !oldSubPartNames.contains(subPartSpec.getName())) {
                            PartitionGroupRecord partGroupRecord =
                                buildPartitionGroupRecord(subPartSpec.getName(), tableGroupId, targetDbCount, i);
                            inVisiblePartitionGroups.add(partGroupRecord);
                            i++;
                        }
                    }
                }
            }
        }

        setInvisiblePartitionGroups(inVisiblePartitionGroups);
    }

    private PartitionGroupRecord buildPartitionGroupRecord(String partName,
                                                           Long tableGroupId,
                                                           int targetDbCount,
                                                           int index) {
        PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
        partitionGroupRecord.visible = 0;
        partitionGroupRecord.partition_name = partName;
        partitionGroupRecord.tg_id = tableGroupId;
        partitionGroupRecord.phy_db = getTargetGroupDetailInfoExRecords().get(index % targetDbCount).phyDbName;
        partitionGroupRecord.locality = "";
        partitionGroupRecord.pax_group_id = 0L;
        return partitionGroupRecord;
    }

    // todo luoyanxin
    // instead of use the backfill way for the neighbor partition,
    // we could label the partitions to be deleted as readonly, we then remove them
    // if drop p3,p5,p7, here return p4,p7
    private Map<String, List<Long>> getTheNeighborPartitionsOfDelPartitions(PartitionByDefinition partByDef,
                                                                            PartitionByDefinition subPartByDef,
                                                                            boolean isSubPartition) {
        Map<String, Set<Long>> neighborPositions = new TreeMap<>(String::compareToIgnoreCase);
        Map<String, List<Long>> neighborPositionList = new TreeMap<>(String::compareToIgnoreCase);

        boolean isRangePartition = partByDef.getStrategy() == PartitionStrategy.RANGE
            || partByDef.getStrategy() == PartitionStrategy.RANGE_COLUMNS;
        boolean isListPartition = partByDef.getStrategy() == PartitionStrategy.LIST
            || partByDef.getStrategy() == PartitionStrategy.LIST_COLUMNS;

        boolean isRangeSubpartition = true, isListSubpartition = true;
        if (subPartByDef != null) {
            isRangeSubpartition = subPartByDef.getStrategy() == PartitionStrategy.RANGE
                || subPartByDef.getStrategy() == PartitionStrategy.RANGE_COLUMNS;
            isListSubpartition = subPartByDef.getStrategy() == PartitionStrategy.LIST
                || subPartByDef.getStrategy() == PartitionStrategy.LIST_COLUMNS;
        }

        if (isRangePartition && isRangeSubpartition) {
            if (subPartByDef != null) {
                //  Two-level Partitioned Table
                for (Map.Entry<String, List<String>> entry : getOldPartitionNamesByLevel().entrySet()) {
                    String partName = entry.getKey();
                    List<String> subPartNames = entry.getValue();

                    PartitionSpec partSpec = partByDef.getPartitionByPartName(partName);

                    Set<Long> positions = neighborPositions.get(partName);
                    if (positions == null) {
                        positions = new HashSet<>();
                        neighborPositions.put(partName, positions);
                    }

                    if (partSpec.getSubPartitions().size() != subPartNames.size()) {
                        // Drop part of subpartitions only
                        for (String subPartName : subPartNames) {
                            PartitionSpec subPartSpec = partSpec.getSubPartitionBySubPartName(subPartName);
                            Long posOfSubPartToBeDropped = subPartSpec.getPosition();

                            if (positions.contains(posOfSubPartToBeDropped)) {
                                positions.remove(posOfSubPartToBeDropped);
                            }

                            if (posOfSubPartToBeDropped < partSpec.getSubPartitions().size()) {
                                positions.add(posOfSubPartToBeDropped);
                            }
                        }
                    } else {
                        // Drop the whole partition, so add part name only for now.
                    }
                }
            } else {
                // One-level Partitioned Table
                Set<Long> positions = neighborPositions.get(ROOT_PART_NAME);
                if (positions == null) {
                    positions = new HashSet<>();
                    neighborPositions.put(ROOT_PART_NAME, positions);
                }

                for (String partName : getOldPartitionNamesByLevel().keySet()) {
                    PartitionSpec partSpec = partByDef.getPartitionByPartName(partName);
                    Long posOfPartToBeDropped = partSpec.getPosition();

                    if (positions.contains(posOfPartToBeDropped)) {
                        positions.remove(posOfPartToBeDropped);
                    }

                    if (posOfPartToBeDropped < partByDef.getPartitions().size()) {
                        positions.add(posOfPartToBeDropped);
                    }
                }
            }

            if (GeneralUtil.isNotEmpty(neighborPositions)) {
                for (Map.Entry<String, Set<Long>> entry : neighborPositions.entrySet()) {
                    String partName = entry.getKey();
                    Set<Long> positions = entry.getValue();

                    if (GeneralUtil.isEmpty(positions) && (subPartByDef == null || isSubPartition)) {
                        continue;
                    }

                    PartitionSpec partSpec = partByDef.getPartitionByPartName(partName);

                    List<Long> finalPositions;
                    PartitionSpec lastPart = null;

                    if (TStringUtil.equalsIgnoreCase(partName, ROOT_PART_NAME)) {
                        // One-level Partitioned Table
                        int totalParts = partByDef.getPartitions().size();
                        lastPart = partByDef.getNthPartition(totalParts);

                        finalPositions = neighborPositionList.get(ROOT_PART_NAME);
                        if (finalPositions == null) {
                            finalPositions = new ArrayList<>();
                            neighborPositionList.put(ROOT_PART_NAME, finalPositions);
                        }
                    } else {
                        // Two-level Partitioned Table
                        if (GeneralUtil.isNotEmpty(positions)) {
                            int totalSubParts = partSpec.getSubPartitions().size();
                            lastPart = partSpec.getNthSubPartition(totalSubParts);
                        } else {
                            Long pos = partSpec.getPosition();
                            if (pos < partByDef.getPartitions().size()) {
                                partSpec = partByDef.getNthPartition(pos.intValue() + 1);
                                partName = partSpec.getName();
                            }
                        }

                        finalPositions = neighborPositionList.get(partName);
                        if (finalPositions == null) {
                            finalPositions = new ArrayList<>();
                            neighborPositionList.put(partName, finalPositions);
                        }
                    }

                    if (lastPart != null) {
                        List<Long> sortedPositions = positions.stream().collect(Collectors.toList());
                        Collections.sort(sortedPositions);

                        for (int i = 0; i < sortedPositions.size() - 1; i++) {
                            if (sortedPositions.get(i) + 1 == sortedPositions.get(i + 1)) {
                                continue;
                            } else {
                                finalPositions.add(sortedPositions.get(i));
                            }
                        }

                        if (!sortedPositions.get(sortedPositions.size() - 1).equals(lastPart.getPosition())) {
                            finalPositions.add(sortedPositions.get(sortedPositions.size() - 1));
                        }
                    } else {
                        // Use all subpartitions of next partition
                        for (PartitionSpec subPartSpec : partSpec.getSubPartitions()) {
                            finalPositions.add(subPartSpec.getPosition() - 1);
                        }
                    }
                }
            }
        } else if (isListPartition && isListSubpartition) {
            //FIXME @chengbi, should process the default partition as the next Neighbor partition
        }

        return neighborPositionList;
    }

    public Map<String, List<String>> getOldPartitionNamesByLevel() {
        return oldPartitionNamesByLevel;
    }

    public void setOldPartitionNamesByLevel(Map<String, List<String>> oldPartitionNamesByLevel) {
        this.oldPartitionNamesByLevel = oldPartitionNamesByLevel;
    }
}
