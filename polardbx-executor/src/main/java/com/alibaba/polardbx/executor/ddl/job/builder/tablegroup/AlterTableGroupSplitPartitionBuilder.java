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
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;

import java.sql.Connection;
import java.util.List;

public class AlterTableGroupSplitPartitionBuilder extends AlterTableGroupBaseBuilder {

    public AlterTableGroupSplitPartitionBuilder(DDL ddl, AlterTableGroupSplitPartitionPreparedData preparedData,
                                                ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    protected void generateNewPhysicalTableNames(List<String> allLogicalTableNames) {
        AlterTableGroupSplitPartitionPreparedData splitData = (AlterTableGroupSplitPartitionPreparedData) preparedData;
        if (splitData.isSplitSubPartition()) {
            TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(splitData.getSchemaName()).getTableGroupInfoManager();
            TableGroupConfig tableGroupConfig =
                tableGroupInfoManager.getTableGroupConfigByName(splitData.getTableGroupName());
            String firstTableName = tableGroupConfig.getTables().get(0);
            PartitionInfo firstPartitionInfo =
                OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                    .getPartitionInfo(firstTableName);
            PartitionByDefinition subPartBy = firstPartitionInfo.getPartitionBy().getSubPartitionBy();
            if (subPartBy != null && subPartBy.isUseSubPartTemplate()) {
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
                            minPostfix[0] =
                                tableGroupRecord.getInited() - preparedData.getInvisiblePartitionGroups().size();
                            minPostfix[0] = Math.max(minPostfix[0], 0);
                            PartitionInfo partitionInfo =
                                OptimizerContext.getContext(schemaName).getPartitionInfoManager()
                                    .getPartitionInfo(tableName);
                            List<String> physicalTables = PartitionInfoUtil
                                .getNextNPhyTableNames(partitionInfo,
                                    preparedData.getInvisiblePartitionGroups().size(), minPostfix);
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
                return;
            }
        }
        super.generateNewPhysicalTableNames(allLogicalTableNames);
    }
}
