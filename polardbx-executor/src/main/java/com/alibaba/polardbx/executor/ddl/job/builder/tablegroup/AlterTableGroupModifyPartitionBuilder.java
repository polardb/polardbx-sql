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

import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;

import java.util.List;

public class AlterTableGroupModifyPartitionBuilder extends AlterTableGroupBaseBuilder {

    public AlterTableGroupModifyPartitionBuilder(DDL ddl, AlterTableGroupModifyPartitionPreparedData preparedData,
                                                 ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public AlterTableGroupItemPreparedData createAlterTableGroupItemPreparedData(String tableName,
                                                                                 List<GroupDetailInfoExRecord> groupDetailInfoExRecords) {
        TableMeta tableMeta =
            getExecutionContext().getSchemaManager(getPreparedData().getSchemaName()).getTable(tableName);
        if (tableMeta.withCci()) {
            TableValidator.validateTableWithCCI(getExecutionContext(), getPreparedData().getTaskType());
        }

        AlterTableGroupItemPreparedData alterTableGroupItemPreparedData =
            new AlterTableGroupItemPreparedData(preparedData.getSchemaName(), tableName);
        PartitionInfo
            partitionInfo = OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
            .getPartitionInfo(tableName);
        PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPhysicalPartitions().get(0);
        alterTableGroupItemPreparedData.setDefaultPartitionSpec(partitionSpec);
        alterTableGroupItemPreparedData.setGroupDetailInfoExRecords(groupDetailInfoExRecords);
        alterTableGroupItemPreparedData.setTableGroupName(preparedData.getTableGroupName());
        alterTableGroupItemPreparedData.setNewPhyTables(getNewPhyTables(tableName));
        //here the oldPartition is tread as the source table of backfill task
        alterTableGroupItemPreparedData.setOldPartitionNames(preparedData.getNewPartitionNames());
        alterTableGroupItemPreparedData.setNewPartitionNames(preparedData.getNewPartitionNames());
        alterTableGroupItemPreparedData.setInvisiblePartitionGroups(preparedData.getInvisiblePartitionGroups());
        alterTableGroupItemPreparedData.setTaskType(preparedData.getTaskType());
        String primaryTableName;
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
        alterTableGroupItemPreparedData.setColumnarIndex(tableMeta.isColumnar());

        return alterTableGroupItemPreparedData;
    }

//    @Override
//    protected void generateNewPhysicalTableNames(List<String> allLogicalTableNames) {
//        final String schemaName = preparedData.getSchemaName();
//        TableGroupRecord tableGroupRecord;
//        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
//            try {
//                conn.setAutoCommit(false);
//                TableGroupAccessor accessor = new TableGroupAccessor();
//                accessor.setConnection(conn);
//                List<TableGroupRecord> tableGroupRecords = accessor
//                    .getTableGroupsBySchemaAndName(schemaName, preparedData.getTableGroupName(), true);
//                assert tableGroupRecords.size() == 1;
//                tableGroupRecord = tableGroupRecords.get(0);
//
//                int[] minPostfix = new int[1];
//                int maxPostfix = 1;
//                for (String tableName : allLogicalTableNames) {
//                    minPostfix[0] = tableGroupRecord.getInited() - preparedData.getNewPartitionNames().size();
//                    minPostfix[0] = Math.max(minPostfix[0], 0);
//                    PartitionInfo partitionInfo =
//                        OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
//                    List<String> physicalTables = PartitionInfoUtil
//                        .getNextNPhyTableNames(partitionInfo, preparedData.getNewPartitionNames().size(), minPostfix);
//                    maxPostfix = Math.max(minPostfix[0], maxPostfix);
//                    newPhysicalTables.put(tableName, physicalTables);
//                }
//                accessor.updateInitedById(tableGroupRecord.getId(), maxPostfix);
//                conn.commit();
//            } finally {
//                MetaDbUtil.endTransaction(conn, PartitionNameUtil.LOGGER);
//            }
//        } catch (Throwable e) {
//            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
//        }
//    }
}
