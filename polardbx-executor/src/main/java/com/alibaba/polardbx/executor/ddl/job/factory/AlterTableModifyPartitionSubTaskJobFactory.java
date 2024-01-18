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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableModifyPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;
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
                                                      Map<String, Pair<String, String>> orderedTargetTableLocations,
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

        //parentPreparedData.prepareInvisiblePartitionGroup();
        List<PartitionGroupRecord> invisiblePartitionGroup = parentPreparedData.getInvisiblePartitionGroups();

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfo(
                parentPreparedData,
                curPartitionInfo,
                false,
                sqlAlterTableSpecNode,
                preparedData.getOldPartitionNames(),
                preparedData.getNewPartitionNames(),
                parentPreparedData.getTableGroupName(),
                null,
                preparedData.getInvisiblePartitionGroups(),
                orderedTargetTableLocations,
                executionContext);

        if (parentPreparedData.isMoveToExistTableGroup()) {
            updateNewPartitionInfoByTargetGroup(parentPreparedData, newPartInfo);
        }

        /**
         * For list partition drop values , eg.
         *      list p1: (v1,v2,v3)
         *      modify partition drop values (v2)
         * its drop values process is combined as two steps::
         *  ==>
         *    1. split p1 to  p1(v1,v3) and p1'(v2)
         *    2. drop partition p1'
         * , so the p1' is the newTempPartitionSpec of all values to be dropped
         */
        if (parentPreparedData.isDropVal()) {
            tempPartitionSpecs = buildAndAppendNewTempPartitionSpec(newPartInfo);
            //newPartInfo.getPartitionBy().getPartitions().add(tempPartitionInfo);
        }
        PartitionInfoUtil.adjustPartitionPositionsForNewPartInfo(newPartInfo);
        PartitionInfoUtil.validatePartitionInfoForDdl(newPartInfo, executionContext);
        return newPartInfo;
    }

}