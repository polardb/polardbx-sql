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
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
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

public class AlterTableGroupModifyPartitionPreparedData extends AlterTableGroupBasePreparedData {
    boolean dropVal = false;
    //the temp partition for the values to be drop
    String tempPartition;

    public AlterTableGroupModifyPartitionPreparedData() {
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
        assert getOldPartitionNames().size() == 1;
        int targetDbCount = getTargetGroupDetailInfoExRecords().size();
        int i = 0;
        List<String> oldParts = new ArrayList<>();
        oldParts.addAll(getOldPartitionNames());
        if (dropVal) {
            List<String> tempPartitionNames = PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, 1);
            tempPartition = tempPartitionNames.get(0);
            oldParts.add(tempPartition);
        }
        for (String oldPartitionName : oldParts) {
            PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
            partitionGroupRecord.visible = 0;
            partitionGroupRecord.partition_name = oldPartitionName;
            partitionGroupRecord.tg_id = tableGroupId;

            partitionGroupRecord.phy_db = getTargetGroupDetailInfoExRecords().get(i % targetDbCount).phyDbName;

            partitionGroupRecord.locality = "";
            partitionGroupRecord.pax_group_id = 0L;
            inVisiblePartitionGroups.add(partitionGroupRecord);
            i++;
        }

        setInvisiblePartitionGroups(inVisiblePartitionGroups);
    }

    public boolean isDropVal() {
        return dropVal;
    }

    public void setDropVal(boolean dropVal) {
        this.dropVal = dropVal;
    }

    public String getTempPartition() {
        return tempPartition;
    }

    public void setTempPartition(String tempPartition) {
        this.tempPartition = tempPartition;
    }
}
