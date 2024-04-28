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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupReorgPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterTableReorgPartitionSubTaskJobFactory extends AlterTableGroupSubTaskJobFactory {

    final AlterTableGroupReorgPartitionPreparedData parentPrepareData;

    public AlterTableReorgPartitionSubTaskJobFactory(DDL ddl,
                                                     AlterTableGroupReorgPartitionPreparedData parentPrepareData,
                                                     AlterTableGroupItemPreparedData preparedData,
                                                     List<PhyDdlTableOperation> phyDdlTableOperations,
                                                     Map<String, List<List<String>>> tableTopology,
                                                     Map<String, Set<String>> targetTableTopology,
                                                     Map<String, Set<String>> sourceTableTopology,
                                                     Map<String, Pair<String, String>> orderedTargetTableLocations,
                                                     String targetPartition,
                                                     boolean skipBackfill,
                                                     ComplexTaskMetaManager.ComplexTaskType taskType,
                                                     ExecutionContext executionContext) {
        super(ddl, parentPrepareData, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology,
            sourceTableTopology, orderedTargetTableLocations, targetPartition, skipBackfill, taskType,
            executionContext);
        this.parentPrepareData = parentPrepareData;
    }

    @Override
    protected PartitionInfo generateNewPartitionInfo() {
        String schemaName = preparedData.getSchemaName();
        String tableGroupName = preparedData.getTableGroupName();
        String tableName = preparedData.getTableName();

        OptimizerContext oc = OptimizerContext.getContext(schemaName);
        OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        SqlNode sqlAlterTableSpecNode;
        if (ddl.getSqlNode() instanceof SqlAlterTableGroup) {
            sqlAlterTableSpecNode = ((SqlAlterTableGroup) ddl.getSqlNode()).getAlters().get(0);
        } else {
            sqlAlterTableSpecNode = ((SqlAlterTable) ddl.getSqlNode()).getAlters().get(0);
        }

        PartitionInfo curPartitionInfo;

        if (TStringUtil.isNotEmpty(tableName)) {
            curPartitionInfo = oc.getPartitionInfoManager().getPartitionInfo(tableName);
        } else if (TStringUtil.isNotEmpty(tableGroupName)) {
            TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigByName(tableGroupName);
            String firstTableName = tableGroupConfig.getAllTables().get(0);
            curPartitionInfo = oc.getPartitionInfoManager().getPartitionInfo(firstTableName);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "unexpected: both table name and table group name are empty");
        }

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfo(
                parentPrepareData,
                curPartitionInfo,
                false,
                sqlAlterTableSpecNode,
                preparedData.getOldPartitionNames(),
                preparedData.getNewPartitionNames(),
                parentPrepareData.getTableGroupName(),
                null,
                preparedData.getInvisiblePartitionGroups(),
                orderedTargetTableLocations,
                executionContext);

        if (parentPrepareData.isMoveToExistTableGroup()) {
            updateNewPartitionInfoByTargetGroup(parentPrepareData, newPartInfo);
        }
        return newPartInfo;
    }

    @Override
    public AlterTableGroupBasePreparedData getParentPrepareData() {
        return parentPrepareData;
    }
}
