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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableMergePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableGroupMergePartition;
import org.apache.calcite.sql.SqlAlterTableMergePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AlterTableMergePartitionSubTaskJobFactory extends AlterTableGroupSubTaskJobFactory {

    final AlterTableMergePartitionPreparedData parentPrepareData;

    public AlterTableMergePartitionSubTaskJobFactory(DDL ddl,
                                                     AlterTableMergePartitionPreparedData parentPrepareData,
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

        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        SqlNode sqlAlterTableSpecNode = ((SqlAlterTable) ddl.getSqlNode()).getAlters().get(0);

        SqlAlterTableMergePartition sqlAlterTableMergePartition =
            (SqlAlterTableMergePartition) sqlAlterTableSpecNode;
        Set<String> mergePartitionsName = sqlAlterTableMergePartition.getOldPartitions().stream()
            .map(o -> Util.last(((SqlIdentifier) (o)).names).toLowerCase()).collect(
                Collectors.toSet());
        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfoForMergeType(curPartitionInfo, preparedData.getInvisiblePartitionGroups(),
                parentPrepareData.getTableGroupName(),
                orderedTargetTableLocations, mergePartitionsName, parentPrepareData.getNewPartitionNames(),
                executionContext);

        if (parentPrepareData.isMoveToExistTableGroup()) {
            updateNewPartitionInfoByTargetGroup(parentPrepareData, newPartInfo);
        }
        PartitionInfoUtil.adjustPartitionPositionsForNewPartInfo(newPartInfo);
        PartitionInfoUtil.validatePartitionInfoForDdl(newPartInfo, executionContext);
        return newPartInfo;
    }

}