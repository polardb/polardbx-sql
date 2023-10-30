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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlSubPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterTableGroupReorgPartitionPreparedData extends AlterTableGroupBasePreparedData {

    public AlterTableGroupReorgPartitionPreparedData() {
    }

    private boolean reorgSubPartition;

    private boolean hasSubPartition;

    private Set<String> oldPartGroupNames;

    private List<SqlPartition> newPartitions;

    private Map<SqlNode, RexNode> partRexInfoCtx;

    public boolean isReorgSubPartition() {
        return reorgSubPartition;
    }

    public void setReorgSubPartition(boolean reorgSubPartition) {
        this.reorgSubPartition = reorgSubPartition;
    }

    public boolean isHasSubPartition() {
        return hasSubPartition;
    }

    public void setHasSubPartition(boolean hasSubPartition) {
        this.hasSubPartition = hasSubPartition;
    }

    public Set<String> getOldPartGroupNames() {
        return oldPartGroupNames;
    }

    public void setOldPartGroupNames(Set<String> oldPartGroupNames) {
        this.oldPartGroupNames = oldPartGroupNames;
    }

    public List<SqlPartition> getNewPartitions() {
        return newPartitions;
    }

    public void setNewPartitions(List<SqlPartition> newPartitions) {
        this.newPartitions = newPartitions;
        List<String> newPartitionNames = new ArrayList<>();
        for (SqlPartition sqlPartition : newPartitions) {
            if (GeneralUtil.isNotEmpty(sqlPartition.getSubPartitions())) {
                for (SqlNode sqlNode : sqlPartition.getSubPartitions()) {
                    String subPartitionName = ((SqlIdentifier) ((SqlSubPartition) sqlNode).getName()).getLastName();
                    newPartitionNames.add(subPartitionName);
                }
            } else {
                String partitionName = ((SqlIdentifier) (sqlPartition).getName()).getLastName();
                newPartitionNames.add(partitionName);
            }
        }
        setNewPartitionNames(newPartitionNames);
    }

    public Map<SqlNode, RexNode> getPartRexInfoCtx() {
        return partRexInfoCtx;
    }

    public void setPartRexInfoCtx(Map<SqlNode, RexNode> partRexInfoCtx) {
        this.partRexInfoCtx = partRexInfoCtx;
    }

    @Override
    public void prepareInvisiblePartitionGroup() {
        if (!reorgSubPartition && hasSubPartition) {
            List<PartitionGroupRecord> inVisiblePartitionGroups = new ArrayList<>();
            TableGroupConfig tableGroupConfig = OptimizerContext.getContext(getSchemaName()).getTableGroupInfoManager()
                .getTableGroupConfigByName(getTableGroupName());
            Long tableGroupId = tableGroupConfig.getTableGroupRecord().getId();
            int targetDbCount = targetGroupDetailInfoExRecords.size();
            int i = 0;
            for (String newPartitionName : getNewPartitionNames()) {
                PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
                partitionGroupRecord.visible = 0;
                partitionGroupRecord.partition_name = newPartitionName;
                partitionGroupRecord.tg_id = tableGroupId;
                partitionGroupRecord.phy_db = targetGroupDetailInfoExRecords.get(i % targetDbCount).phyDbName;
                partitionGroupRecord.locality = "";
                partitionGroupRecord.pax_group_id = 0L;
                inVisiblePartitionGroups.add(partitionGroupRecord);
                i++;
            }

            setInvisiblePartitionGroups(inVisiblePartitionGroups);
        } else {
            super.prepareInvisiblePartitionGroup();
        }
    }
}
