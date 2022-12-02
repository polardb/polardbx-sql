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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class AlterTableGroupBaseBuilder {

    protected final DDL relDdl;
    protected final AlterTableGroupBasePreparedData preparedData;
    protected final ExecutionContext executionContext;

    protected Map<String, List<PhyDdlTableOperation>> newPartitionsPhysicalPlansMap =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, Map<String, List<List<String>>>> tablesTopologyMap =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, AlterTableGroupItemPreparedData> tablesPreparedData =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, Map<String, Set<String>>> sourceTablesTopology = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, Map<String, Set<String>>> targetTablesTopology = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, List<String>> newPhysicalTables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    /**
     * orderedTargetTablesLocations is used to store all the locations of target new added phy tables
     * the list order is map to the new partitions order
     * <p>
     * key:logTb
     * val:
     * val:list of locations
     * a item is [phyTbl, grpKey]
     */
    protected Map<String, List<Pair<String, String>>> orderedTargetTablesLocations = new HashMap<>();

    public AlterTableGroupBaseBuilder(DDL ddl, AlterTableGroupBasePreparedData preparedData,
                                      ExecutionContext executionContext) {
        this.relDdl = ddl;
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    public AlterTableGroupBaseBuilder build() {
        buildTablesPhysicalPlans();
        return this;
    }

    public void buildTablesPhysicalPlans() {
        List<GroupDetailInfoExRecord> groupDetailInfoExRecords = preparedData.getTargetGroupDetailInfoExRecords();
        List<String> allTables = getAllTableNames();
        generateNewPhysicalTableNames(allTables);
        for (String tableName : allTables) {
            AlterTableGroupItemPreparedData alterTableGroupItemPreparedData =
                createAlterTableGroupItemPreparedData(tableName, groupDetailInfoExRecords);
            AlterTableGroupItemBuilder itemBuilder =
                new AlterTableGroupItemBuilder(relDdl, alterTableGroupItemPreparedData, executionContext);
            List<PhyDdlTableOperation> phyDdlTableOperations = itemBuilder.build().getPhysicalPlans();
            tablesTopologyMap.put(tableName, itemBuilder.getTableTopology());
            sourceTablesTopology.put(tableName, itemBuilder.getSourcePhyTables());
            targetTablesTopology.put(tableName, itemBuilder.getTargetPhyTables());
            newPartitionsPhysicalPlansMap.put(tableName, phyDdlTableOperations);
            tablesPreparedData.put(tableName, alterTableGroupItemPreparedData);
            orderedTargetTablesLocations.put(tableName, itemBuilder.getOrderedTargetTableLocations());
        }
    }

    public List<String> getAllTableNames() {
        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager()
                .getTableGroupConfigByName(preparedData.getTableGroupName());
        return tableGroupConfig.getAllTables().stream().map(o -> o.getTableName()).collect(Collectors.toList());
    }

    public Map<String, List<PhyDdlTableOperation>> getNewPartitionsPhysicalPlansMap() {
        return newPartitionsPhysicalPlansMap;
    }

    public Map<String, Map<String, List<List<String>>>> getTablesTopologyMap() {
        return tablesTopologyMap;
    }

    public Map<String, Map<String, Set<String>>> getSourceTablesTopology() {
        return sourceTablesTopology;
    }

    public Map<String, Map<String, Set<String>>> getTargetTablesTopology() {
        return targetTablesTopology;
    }

    public Map<String, AlterTableGroupItemPreparedData> getTablesPreparedData() {
        return tablesPreparedData;
    }

    public Map<String, List<Pair<String, String>>> getOrderedTargetTablesLocations() {
        return orderedTargetTablesLocations;
    }

    public AlterTableGroupItemPreparedData createAlterTableGroupItemPreparedData(String tableName,
                                                                                 List<GroupDetailInfoExRecord> groupDetailInfoExRecords) {

        AlterTableGroupItemPreparedData alterTableGroupItemPreparedData =
            new AlterTableGroupItemPreparedData(preparedData.getSchemaName(), tableName);
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                .getPartitionInfo(tableName);
        PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPartitions().get(0);
        alterTableGroupItemPreparedData.setDefaultPartitionSpec(partitionSpec);
        alterTableGroupItemPreparedData.setGroupDetailInfoExRecords(groupDetailInfoExRecords);
        alterTableGroupItemPreparedData.setTableGroupName(preparedData.getTableGroupName());
        alterTableGroupItemPreparedData.setNewPhyTables(getNewPhyTables(tableName));
        alterTableGroupItemPreparedData.setOldPartitionNames(preparedData.getOldPartitionNames());
        alterTableGroupItemPreparedData.setNewPartitionNames(preparedData.getNewPartitionNames());
        alterTableGroupItemPreparedData.setInvisiblePartitionGroups(preparedData.getInvisiblePartitionGroups());
        alterTableGroupItemPreparedData.setTaskType(preparedData.getTaskType());
        String primaryTableName;
        TableMeta tableMeta = executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(tableName);
        if (tableMeta.isGsi()) {
            //all the gsi table version change will be behavior by primary table
            assert
                tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
            primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
        } else {
            primaryTableName = tableName;
        }
        alterTableGroupItemPreparedData.setPrimaryTableName(primaryTableName);
        alterTableGroupItemPreparedData
            .setTableVersion(
                executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(primaryTableName)
                    .getVersion());

        return alterTableGroupItemPreparedData;
    }

    public AlterTableGroupBasePreparedData getPreparedData() {
        return preparedData;
    }

    public DDL getRelDdl() {
        return relDdl;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public List<String> getNewPhyTables(String tableName) {
        return newPhysicalTables.get(tableName);
    }

    protected void generateNewPhysicalTableNames(List<String> allLogicalTableNames) {
        final String schemaName = preparedData.getSchemaName();
        TableGroupRecord tableGroupRecord;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            try {
                conn.setAutoCommit(false);
                TableGroupAccessor accessor = new TableGroupAccessor();
                accessor.setConnection(conn);
                List<TableGroupRecord> tableGroupRecords = accessor
                    .getTableGroupsBySchemaAndName(schemaName, preparedData.getTableGroupName(), true);
                assert tableGroupRecords.size() == 1;
                tableGroupRecord = tableGroupRecords.get(0);

                int[] minPostfix = new int[1];
                int maxPostfix = 1;
                for (String tableName : allLogicalTableNames) {
                    minPostfix[0] = tableGroupRecord.getInited() - preparedData.getNewPartitionNames().size();
                    minPostfix[0] = Math.max(minPostfix[0], 0);
                    PartitionInfo partitionInfo =
                        OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
                    List<String> physicalTables = PartitionInfoUtil
                        .getNextNPhyTableNames(partitionInfo, preparedData.getNewPartitionNames().size(), minPostfix);
                    maxPostfix = Math.max(minPostfix[0], maxPostfix);
                    newPhysicalTables.put(tableName, physicalTables);
                }
                accessor.updateInitedById(tableGroupRecord.getId(), maxPostfix);
                conn.commit();
            } finally {
                MetaDbUtil.endTransaction(conn, PartitionNameUtil.LOGGER);
            }
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }
}
