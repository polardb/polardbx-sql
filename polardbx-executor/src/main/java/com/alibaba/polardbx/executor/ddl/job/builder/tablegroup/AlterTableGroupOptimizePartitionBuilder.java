package com.alibaba.polardbx.executor.ddl.job.builder.tablegroup;

import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupOptimizePartitionItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupOptimizePartitionPreparedData;
import org.apache.calcite.rel.core.DDL;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlterTableGroupOptimizePartitionBuilder extends AlterTableGroupBaseBuilder {

    protected AlterTableGroupOptimizePartitionPreparedData preparedData;
    protected final Map<String, Long> tableVersions = new HashMap<>();
    protected final Map<String, PhysicalPlanData> phyPlanDataMap = new HashMap<>();

    public AlterTableGroupOptimizePartitionBuilder(DDL ddl, AlterTableGroupOptimizePartitionPreparedData preparedData,
                                                   ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    @Override
    public void buildTablesPhysicalPlans() {
        List<String> allTables = getAllTableNames();
        for (String tableName : allTables) {
            AlterTableGroupOptimizePartitionItemPreparedData itemPreparedData = buildItemPreparedData(tableName);
            AlterTableGroupItemBuilder itemBuilder =
                new AlterTableGroupOptimizePartitionItemBuilder(relDdl, itemPreparedData, executionContext).build();
            tablesTopologyMap.put(tableName, itemBuilder.getTableTopology());
            newPartitionsPhysicalPlansMap.put(tableName, itemBuilder.getPhysicalPlans());
            phyPlanDataMap.put(tableName, itemBuilder.genPhysicalPlanData());
            tableVersions.put(tableName, itemPreparedData.getTableVersion());
        }
    }

    private AlterTableGroupOptimizePartitionItemPreparedData buildItemPreparedData(String tableName) {
        String schemaName = preparedData.getSchemaName();

        AlterTableGroupOptimizePartitionItemPreparedData itemPreparedData =
            new AlterTableGroupOptimizePartitionItemPreparedData(schemaName, tableName);

        itemPreparedData.setSchemaName(schemaName);
        itemPreparedData.setTableName(tableName);
        itemPreparedData.setTableVersion(
            executionContext.getSchemaManager(schemaName).getTable(tableName).getVersion());
        itemPreparedData.setOptimizePartitionNames(preparedData.getOptimizePartitionNames());

        return itemPreparedData;
    }

    public Map<String, Long> getTableVersions() {
        return tableVersions;
    }

    public Map<String, PhysicalPlanData> getPhyPlanDataMap() {
        return phyPlanDataMap;
    }
}
