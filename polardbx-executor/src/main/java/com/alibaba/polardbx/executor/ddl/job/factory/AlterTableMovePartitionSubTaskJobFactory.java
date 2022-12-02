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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableMovePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterTableMovePartitionSubTaskJobFactory extends AlterTableGroupSubTaskJobFactory {

    final AlterTableMovePartitionPreparedData parentPrepareData;

    public AlterTableMovePartitionSubTaskJobFactory(DDL ddl,
                                                    AlterTableMovePartitionPreparedData parentPrepareData,
                                                    AlterTableGroupItemPreparedData preparedData,
                                                    List<PhyDdlTableOperation> phyDdlTableOperations,
                                                    Map<String, List<List<String>>> tableTopology,
                                                    Map<String, Set<String>> targetTableTopology,
                                                    Map<String, Set<String>> sourceTableTopology,
                                                    List<Pair<String, String>> orderedTargetTableLocations,
                                                    String targetPartition,
                                                    boolean skipBackfill,
                                                    ComplexTaskMetaManager.ComplexTaskType taskType,
                                                    ExecutionContext executionContext) {
        super(ddl, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology, sourceTableTopology,
            orderedTargetTableLocations, targetPartition, skipBackfill, taskType, executionContext);
        this.parentPrepareData = parentPrepareData;
    }

    @Override
    protected PartitionInfo generateNewPartitionInfo() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();
        String tableGroupName = preparedData.getTableGroupName();

        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        SqlNode sqlAlterTableSpecNode = ((SqlAlterTable) ddl.getSqlNode()).getAlters().get(0);

        PartitionInfo newPartInfo =
            AlterTableGroupSnapShotUtils
                .getNewPartitionInfoForMoveType(curPartitionInfo,
                    ((SqlAlterTableMovePartition) sqlAlterTableSpecNode).getTargetPartitions(),
                    tableGroupName,
                    orderedTargetTableLocations,
                    executionContext);

        if (parentPrepareData.isMoveToExistTableGroup()) {
            updateNewPartitionInfoByTargetGroup(parentPrepareData, newPartInfo);
        }
        PartitionInfoUtil.adjustPartitionPositionsForNewPartInfo(newPartInfo);
        PartitionInfoUtil.validatePartitionInfoForDdl(newPartInfo, executionContext);
        return newPartInfo;
    }

}