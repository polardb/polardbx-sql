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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.builder.AlterTableDiscardTableSpaceBuilder;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMovePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class AlterTableGroupMovePartitionBuilder extends AlterTableGroupBaseBuilder {

    protected Map<String, List<PhyDdlTableOperation>> discardTableSpacePhysicalPlansMap =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    Map<String, Map<String, Pair<String, String>>> tbPtbGroupMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public AlterTableGroupMovePartitionBuilder(DDL ddl, AlterTableGroupMovePartitionPreparedData preparedData,
                                               ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public void buildTablesPhysicalPlans() {
        List<GroupDetailInfoExRecord> groupDetailInfoExRecords = preparedData.getTargetGroupDetailInfoExRecords();
        List<String> allTables = getAllTableNames();
        generateNewPhysicalTableNames(allTables);

        for (String tableName : allTables) {
            AlterTableGroupItemPreparedData alterTableGroupItemPreparedData =
                createAlterTableGroupItemPreparedData(tableName, groupDetailInfoExRecords);
            AlterTableGroupItemBuilder itemBuilder =
                new AlterTableGroupMovePartitionItemBuilder(relDdl, alterTableGroupItemPreparedData, executionContext);
            List<PhyDdlTableOperation> phyDdlTableOperations = itemBuilder.build().getPhysicalPlans();
            tablesTopologyMap.put(tableName, itemBuilder.getTableTopology());
            sourceTablesTopology.put(tableName, itemBuilder.getSourcePhyTables());
            targetTablesTopology.put(tableName, itemBuilder.getTargetPhyTables());
            newPartitionsPhysicalPlansMap.put(tableName, phyDdlTableOperations);
            tablesPreparedData.put(tableName, alterTableGroupItemPreparedData);
            orderedTargetTablesLocations.put(tableName, itemBuilder.getOrderedTargetTableLocations());

            Map<String, List<List<String>>> tablesTopology = itemBuilder.getTableTopology();
            AlterTableDiscardTableSpaceBuilder discardTableSpaceBuilder =
                AlterTableDiscardTableSpaceBuilder.createBuilder(
                    preparedData.getSchemaName(), tableName, tablesTopology, executionContext);
            discardTableSpacePhysicalPlansMap.put(tableName, discardTableSpaceBuilder.build().getPhysicalPlans());

            if (preparedData.isUsePhysicalBackfill()) {
                tbPtbGroupMap.put(tableName, new HashMap<>());
                for (Map.Entry<String, Set<String>> srcPhyTbInfo : itemBuilder.getSourcePhyTables().entrySet()) {
                    String srcGroupKey = srcPhyTbInfo.getKey();

                    assert srcGroupKey != null;

                    for (String phyTbName : srcPhyTbInfo.getValue()) {
                        String tarGroupKey = null;
                        for (Map.Entry<String, Set<String>> tarPhyTbInfo : itemBuilder.getTargetPhyTables()
                            .entrySet()) {
                            if (tarPhyTbInfo.getValue().contains(phyTbName)) {
                                tarGroupKey = tarPhyTbInfo.getKey();
                                break;
                            }
                        }

                        tbPtbGroupMap.get(tableName).put(phyTbName, Pair.of(srcGroupKey, tarGroupKey));
                    }
                }
            }
        }
    }

    @Override
    public List<String> getNewPhyTables(String tableName) {
        List<String> newPhyTables = new ArrayList<>();
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                .getPartitionInfo(tableName);
        List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPartitions();
        for (PartitionGroupRecord partitionGroupRecord : preparedData.getInvisiblePartitionGroups()) {
            PartitionSpec partitionSpec =
                partitionSpecs.stream().filter(o -> o.getName().equalsIgnoreCase(partitionGroupRecord.partition_name))
                    .findFirst().orElse(null);
            if (partitionSpec == null && partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
                for (PartitionSpec ps : partitionSpecs) {
                    partitionSpec = ps.getSubPartitions().stream()
                        .filter(sp -> sp.getName().equalsIgnoreCase(partitionGroupRecord.partition_name)).findFirst()
                        .orElse(null);
                    if (partitionSpec != null) {
                        break;
                    }
                }
            }
            assert partitionSpec != null;
            newPhyTables.add(partitionSpec.getLocation().getPhyTableName());
        }
        return newPhyTables;
    }

    public Map<String, List<PhyDdlTableOperation>> getDiscardTableSpacePhysicalPlansMap() {
        return discardTableSpacePhysicalPlansMap;
    }

    public Map<String, Map<String, Pair<String, String>>> getTbPtbGroupMap() {
        return tbPtbGroupMap;
    }
}
