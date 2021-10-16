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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSetTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class AlterTableSetTableGroupBuilder extends DdlPhyPlanBuilder {

    protected final AlterTableSetTableGroupPreparedData preparedData;

    protected Map<String, Set<String>> sourceTableTopology = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, Set<String>> targetTableTopology = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected List<PartitionGroupRecord> newPartitionRecords = new ArrayList<>();

    public AlterTableSetTableGroupBuilder(DDL ddl, AlterTableSetTableGroupPreparedData preparedData,
                                          ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    @Override
    public AlterTableSetTableGroupBuilder build() {
        buildSqlTemplate();
        buildChangedTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
        buildPhysicalPlans(preparedData.getTableName());
        built = true;
        return this;
    }

    @Override
    protected void buildTableRuleAndTopology() {
    }

    @Override
    protected void buildPhysicalPlans() {
    }

    @Override
    public void buildChangedTableTopology(String schemaName, String tableName) {
        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(preparedData.getTableGroupName());
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        List<PartitionGroupRecord> partitionGroupRecords = tableGroupConfig.getPartitionGroupRecords();
        tableTopology = new HashMap<>();
        for (PartitionGroupRecord partitionGroupRecord : partitionGroupRecords) {
            PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPartitions().stream()
                .filter(o -> o.getName().equalsIgnoreCase(partitionGroupRecord.getPartition_name())).findFirst()
                .orElse(null);
            if (partitionSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "can't find the partition:" + partitionGroupRecord.getPartition_name());
            }
            String targetPhyDb = partitionGroupRecord.getPhy_db();
            String targetGroupKey = GroupInfoUtil.buildGroupNameFromPhysicalDb(targetPhyDb);
            if (!partitionSpec.getLocation().getGroupKey()
                .equalsIgnoreCase(targetGroupKey)) {
                String targetGroupName = targetGroupKey;
                String sourceGroupName = partitionSpec.getLocation().getGroupKey();
                String phyTable = partitionSpec.getLocation().getPhyTableName();
                sourceTableTopology.computeIfAbsent(sourceGroupName, k -> new HashSet<>())
                    .add(partitionSpec.getLocation().getPhyTableName());
                targetTableTopology.computeIfAbsent(targetGroupName, k -> new HashSet<>())
                    .add(partitionSpec.getLocation().getPhyTableName());

                List<String> phyTables = new ArrayList<>();
                phyTables.add(phyTable);
                tableTopology.computeIfAbsent(targetGroupName, k -> new ArrayList<>()).add(phyTables);
                newPartitionRecords.add(partitionGroupRecord);
            }
        }
    }

    @Override
    protected void buildSqlTemplate() {
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                .getPartitionInfo(preparedData.getTableName());
        PartitionLocation location = partitionInfo.getPartitionBy().getPartitions().get(0).getLocation();
        String createTableStr = AlterTableGroupUtils
            .fetchCreateTableDefinition(relDdl, executionContext, location.getGroupKey(), location.getPhyTableName(),
                preparedData.getSchemaName());
        sqlTemplate = AlterTableGroupUtils.getSqlTemplate(createTableStr, executionContext);
    }

    public Map<String, Set<String>> getSourceTableTopology() {
        return sourceTableTopology;
    }

    public Map<String, Set<String>> getTargetTableTopology() {
        return targetTableTopology;
    }

    public List<PartitionGroupRecord> getNewPartitionRecords() {
        return newPartitionRecords;
    }
}
