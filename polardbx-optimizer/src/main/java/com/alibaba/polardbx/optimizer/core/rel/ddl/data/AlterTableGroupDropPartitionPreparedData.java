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
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AlterTableGroupDropPartitionPreparedData extends AlterTableGroupBasePreparedData {

    public AlterTableGroupDropPartitionPreparedData() {
    }

    @Override
    public void prepareInvisiblePartitionGroup() {
        List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
            .getTableGroupConfigByName(getTableGroupName());
        assert tableGroupConfig != null && GeneralUtil.isNotEmpty(tableGroupConfig.getAllTables());
        String firstTbName = tableGroupConfig.getAllTables().get(0).getLogTbRec().getTableName();
        PartitionInfo firstTblPartInfo =
            OptimizerContext.getContext(getSchemaName()).getPartitionInfoManager().getPartitionInfo(firstTbName);
        Long tableGroupId = firstTblPartInfo.getTableGroupId();
        List<Long> delPartPosList = getTheNeighborPartitionsOfDelPartitions();
        if (delPartPosList.size() > 0) {
            int targetDbCount = getTargetGroupDetailInfoExRecords().size();
            int i = 0;
            for (Long pos : delPartPosList) {
                PartitionSpec partitionSpec = firstTblPartInfo.getPartitionBy().getPartitions().get(pos.intValue());
                PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
                partitionGroupRecord.visible = 0;
                partitionGroupRecord.partition_name = partitionSpec.getName();
                partitionGroupRecord.tg_id = tableGroupId;

                partitionGroupRecord.phy_db = getTargetGroupDetailInfoExRecords().get(i % targetDbCount).phyDbName;

                partitionGroupRecord.locality = "";
                partitionGroupRecord.pax_group_id = 0L;
                inVisiblePartitionGroups.add(partitionGroupRecord);
                i++;
            }
        }
        setInvisiblePartitionGroups(inVisiblePartitionGroups);
    }

    //todo luoyanxin
    // instend of use the backfill way for the neighbor partition,
    // we could label the partitions to be deleted as readonly, we then remove them
    // if drop p3,p5,p7, here return p4,p7
    public List<Long> getTheNeighborPartitionsOfDelPartitions() {
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
            .getTableGroupConfigByName(getTableGroupName());
        assert tableGroupConfig != null && GeneralUtil.isNotEmpty(tableGroupConfig.getAllTables());
        String firstTbName = tableGroupConfig.getAllTables().get(0).getLogTbRec().getTableName();
        PartitionInfo firstTblPartInfo =
            OptimizerContext.getContext(getSchemaName()).getPartitionInfoManager().getPartitionInfo(firstTbName);

        Set<Long> neighborPartitions = new HashSet<>();

        for (String delPart : getOldPartitionNames()) {
            if (firstTblPartInfo.getPartitionBy().getStrategy() == PartitionStrategy.RANGE
                || firstTblPartInfo.getPartitionBy().getStrategy() == PartitionStrategy.RANGE_COLUMNS) {
                PartitionSpec delPartSpce =
                    firstTblPartInfo.getPartitionBy().getPartitionByPartName(delPart);
                Long posOfPartToBeDrop = delPartSpce.getPosition();
                if (neighborPartitions.contains(posOfPartToBeDrop)) {
                    neighborPartitions.remove(posOfPartToBeDrop);
                }
                if (posOfPartToBeDrop < firstTblPartInfo.getPartitionBy().getPartitions().size()) {
                    neighborPartitions.add(posOfPartToBeDrop);
                }
            } else if (firstTblPartInfo.getPartitionBy().getStrategy() == PartitionStrategy.LIST
                || firstTblPartInfo.getPartitionBy().getStrategy() == PartitionStrategy.LIST_COLUMNS) {
                //FIXME @chengbi, should process the default partition as the next Neighbor partition
            }
        }
        List<Long> neighborPartitionList = new ArrayList<>();
        if (!neighborPartitions.isEmpty() && (firstTblPartInfo.getPartitionBy().getStrategy() == PartitionStrategy.RANGE
            || firstTblPartInfo.getPartitionBy().getStrategy() == PartitionStrategy.RANGE_COLUMNS)) {
            List<Long> delPartPosList = neighborPartitions.stream().collect(Collectors.toList());
            Collections.sort(delPartPosList);
            for (int i = 0; i < delPartPosList.size() - 1; i++) {
                if (delPartPosList.get(i) + 1 == delPartPosList.get(i + 1)) {
                    continue;
                } else {
                    neighborPartitionList.add(delPartPosList.get(i));
                }
            }
            neighborPartitionList.add(delPartPosList.get(delPartPosList.size() - 1));
        }
        return neighborPartitionList;
    }
}
