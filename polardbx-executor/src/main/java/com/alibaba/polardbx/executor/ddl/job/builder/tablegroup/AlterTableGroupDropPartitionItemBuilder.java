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

package com.alibaba.polardbx.executor.ddl.job.builder.tablegroup;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterTableGroupDropPartitionItemBuilder extends AlterTableGroupItemBuilder {

    public AlterTableGroupDropPartitionItemBuilder(DDL ddl,
                                                   AlterTableGroupItemPreparedData preparedData,
                                                   ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public Map<String, Set<String>> getSourcePhyTables() {
        if (GeneralUtil.isEmpty(sourcePhyTables) && GeneralUtil
            .isNotEmpty(preparedData.getInvisiblePartitionGroups())) {
            PartitionInfo partitionInfo =
                OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                    .getPartitionInfo(preparedData.getTableName());
            for (PartitionGroupRecord neighbourPartition : preparedData.getInvisiblePartitionGroups()) {
                for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                    if (partitionSpec.getName().equalsIgnoreCase(neighbourPartition.getPartition_name())) {
                        PartitionLocation location = partitionSpec.getLocation();
                        sourcePhyTables.computeIfAbsent(location.getGroupKey(), o -> new HashSet<>())
                            .add(location.getPhyTableName());
                        break;
                    }
                }
            }
        }
        return sourcePhyTables;
    }

    @Override
    protected void buildNewTableTopology(String schemaName, String tableName) {
        tableTopology = new HashMap<>();
        List<GroupDetailInfoExRecord> groupDetailInfoExRecords = preparedData.getGroupDetailInfoExRecords();
        int i = 0;
        for (String newPhyTableName : preparedData.getNewPhyTables()) {
            GroupDetailInfoExRecord groupDetailInfoExRecord = groupDetailInfoExRecords.get(i++);
            List<String> phyTables = new ArrayList<>();
            phyTables.add(newPhyTableName);
            tableTopology.computeIfAbsent(groupDetailInfoExRecord.getGroupName(), o -> new ArrayList<>())
                .add(phyTables);
            targetPhyTables.computeIfAbsent(groupDetailInfoExRecord.getGroupName(), o -> new HashSet<>())
                .add(newPhyTableName);
            orderedTargetTableLocations.add(new Pair<>(newPhyTableName, groupDetailInfoExRecord.getGroupName()));
            if (i >= groupDetailInfoExRecords.size()) {
                i = 0;
            }
        }
    }
}
