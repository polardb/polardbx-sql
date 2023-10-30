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

import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupTruncatePartitionItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupTruncatePartitionPreparedData;
import org.apache.calcite.rel.core.DDL;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterTableGroupTruncatePartitionBuilder extends AlterTableGroupBaseBuilder {

    protected AlterTableGroupTruncatePartitionPreparedData preparedData;
    protected final Map<String, Long> tableVersions = new HashMap<>();
    protected final Map<String, PhysicalPlanData> phyPlanDataMap = new HashMap<>();

    public AlterTableGroupTruncatePartitionBuilder(DDL ddl, AlterTableGroupTruncatePartitionPreparedData preparedData,
                                                   ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    @Override
    public void buildTablesPhysicalPlans() {
        List<String> allTables = getAllTableNames();
        for (String tableName : allTables) {
            AlterTableGroupTruncatePartitionItemPreparedData itemPreparedData = buildItemPreparedData(tableName);
            AlterTableGroupItemBuilder itemBuilder =
                new AlterTableGroupTruncatePartitionItemBuilder(relDdl, itemPreparedData, executionContext).build();
            tablesTopologyMap.put(tableName, itemBuilder.getTableTopology());
            newPartitionsPhysicalPlansMap.put(tableName, itemBuilder.getPhysicalPlans());
            phyPlanDataMap.put(tableName, itemBuilder.genPhysicalPlanData());
            tableVersions.put(tableName, itemPreparedData.getTableVersion());
        }
    }

    private AlterTableGroupTruncatePartitionItemPreparedData buildItemPreparedData(String tableName) {
        String schemaName = preparedData.getSchemaName();

        AlterTableGroupTruncatePartitionItemPreparedData itemPreparedData =
            new AlterTableGroupTruncatePartitionItemPreparedData(schemaName, tableName);

        itemPreparedData.setSchemaName(schemaName);
        itemPreparedData.setTableName(tableName);
        itemPreparedData.setTableVersion(
            executionContext.getSchemaManager(schemaName).getTable(tableName).getVersion());
        itemPreparedData.setTruncatePartitionNames(preparedData.getTruncatePartitionNames());

        return itemPreparedData;
    }

    public Map<String, Long> getTableVersions() {
        return tableVersions;
    }

    public Map<String, PhysicalPlanData> getPhyPlanDataMap() {
        return phyPlanDataMap;
    }
}
