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
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AlterTableGroupSplitPartitionByHotValueSubTaskJobFactory extends AlterTableGroupSubTaskJobFactory {

    final AlterTableGroupSplitPartitionByHotValuePreparedData parentPrepareData;

    public AlterTableGroupSplitPartitionByHotValueSubTaskJobFactory(DDL ddl,
                                                                    AlterTableGroupSplitPartitionByHotValuePreparedData parentPrepareData,
                                                                    AlterTableGroupItemPreparedData preparedData,
                                                                    List<PhyDdlTableOperation> phyDdlTableOperations,
                                                                    Map<String, List<List<String>>> tableTopology,
                                                                    Map<String, Set<String>> targetTableTopology,
                                                                    Map<String, Set<String>> sourceTableTopology,
                                                                    List<Pair<String, String>> orderedTargetTableLocations,
                                                                    String targetPartition,
                                                                    boolean skipBackfill,
                                                                    ExecutionContext executionContext) {
        super(ddl, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology, sourceTableTopology,
            orderedTargetTableLocations, targetPartition, skipBackfill, executionContext);
        this.parentPrepareData = parentPrepareData;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();
        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        PartitionInfo newPartitionInfo = generateNewPartitionInfo();
        if (!schemaChange(curPartitionInfo, newPartitionInfo)) {
            parentPrepareData.setSkipSplit(true);
            return new TransientDdlJob();
        }

        return super.doCreate();
    }

    @Override
    protected PartitionInfo generateNewPartitionInfo() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();

        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfoForSplitPartitionByHotValue(curPartitionInfo, parentPrepareData,
                orderedTargetTableLocations, executionContext);
        //checkPartitionCount(newPartInfo);
        return newPartInfo;
    }

    private boolean schemaChange(PartitionInfo curPartInfo, PartitionInfo newPartInfo) {
        if (curPartInfo.getPartitionBy().getPartitions().size() != newPartInfo.getPartitionBy().getPartitions()
            .size()) {
            return true;
        }
        for (int i = 0; i < curPartInfo.getPartitionBy().getPartitions().size(); i++) {
            PartitionSpec curPartSpec = curPartInfo.getPartitionBy().getPartitions().get(i);
            PartitionSpec newPartSpec = newPartInfo.getPartitionBy().getPartitions().get(i);
            if (curPartSpec.getBoundSpaceComparator()
                .compare(curPartSpec.getBoundSpec().getSingleDatum(), newPartSpec.getBoundSpec().getSingleDatum())
                != 0) {
                return true;
            }
        }
        return false;
    }

}