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
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableModifyPartitionValues;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterTableModifyPartitionSubTaskJobFactory extends AlterTableGroupModifyPartitionSubTaskJobFactory {

    public AlterTableModifyPartitionSubTaskJobFactory(DDL ddl,
                                                      AlterTableModifyPartitionPreparedData parentPrepareData,
                                                      AlterTableGroupItemPreparedData preparedData,
                                                      List<PhyDdlTableOperation> phyDdlTableOperations,
                                                      Map<String, List<List<String>>> tableTopology,
                                                      Map<String, Set<String>> targetTableTopology,
                                                      Map<String, Set<String>> sourceTableTopology,
                                                      List<Pair<String, String>> orderedTargetTableLocations,
                                                      String targetPartition,
                                                      boolean skipBackfill,
                                                      ExecutionContext executionContext) {
        super(ddl, parentPrepareData, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology,
            sourceTableTopology,
            orderedTargetTableLocations, targetPartition, skipBackfill, executionContext);
    }

    protected SqlAlterTableModifyPartitionValues getSqlNode() {
        SqlAlterTable sqlAlterTable = (SqlAlterTable) ddl.getSqlNode();
        return (SqlAlterTableModifyPartitionValues) sqlAlterTable.getAlters().get(0);
    }

    @Override
    protected PartitionInfo generateNewPartitionInfo() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();

        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        SqlNode sqlAlterTableSpecNode = ((SqlAlterTable) ddl.getSqlNode()).getAlters().get(0);

        PartitionInfo newPartInfo =
            AlterTableGroupSnapShotUtils
                .getNewPartitionInfoForModifyPartition(curPartitionInfo, sqlAlterTableSpecNode,
                    parentPreparedData.getPartBoundExprInfo(),
                    orderedTargetTableLocations,
                    executionContext);

        if (parentPreparedData.isMoveToExistTableGroup()) {
            updateNewPartitionInfoByTargetGroup(parentPreparedData, newPartInfo);
        }

        if (parentPreparedData.isDropVal()) {
            tempPartitionInfo = buildNewTempPartitionSpec(newPartInfo);
            newPartInfo.getPartitionBy().getPartitions().add(tempPartitionInfo);
        }
        PartitionInfoUtil.adjustPartitionPositionsForNewPartInfo(newPartInfo);
        PartitionInfoUtil.validatePartitionInfoForDdl(newPartInfo, executionContext);
        return newPartInfo;
    }

}