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

import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RefreshDbTopologyPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RefreshTopologyBuilder extends AlterTableGroupBaseBuilder {

    public RefreshTopologyBuilder(DDL ddl, RefreshDbTopologyPreparedData preparedData,
                                  ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public AlterTableGroupItemPreparedData createAlterTableGroupItemPreparedData(String tableName,
                                                                                 List<GroupDetailInfoExRecord> groupDetailInfoExRecords) {

        AlterTableGroupItemPreparedData alterTableGroupItemPreparedData =
            new AlterTableGroupItemPreparedData(preparedData.getSchemaName(), tableName);
        PartitionInfo
            partitionInfo = OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
            .getPartitionInfo(tableName);
        Random random = new Random();
        int selIndex = random.nextInt(partitionInfo.getPartitionBy().getPartitions().size());
        PartitionSpec sourcePartitionSpec = partitionInfo.getPartitionBy().getPartitions().get(selIndex);
        List<String> sourcePartNames = new ArrayList<>(1);
        sourcePartNames.add(sourcePartitionSpec.getName());
        alterTableGroupItemPreparedData.setDefaultPartitionSpec(sourcePartitionSpec);
        alterTableGroupItemPreparedData.setGroupDetailInfoExRecords(groupDetailInfoExRecords);
        alterTableGroupItemPreparedData.setTableGroupName(preparedData.getTableGroupName());
        alterTableGroupItemPreparedData.setNewPhyTables(getNewPhyTables(partitionInfo));
        alterTableGroupItemPreparedData.setOldPartitionNames(sourcePartNames);
        alterTableGroupItemPreparedData.setNewPartitionNames(preparedData.getNewPartitionNames());
        alterTableGroupItemPreparedData.setInvisiblePartitionGroups(preparedData.getInvisiblePartitionGroups());
        alterTableGroupItemPreparedData.setTaskType(preparedData.getTaskType());

        return alterTableGroupItemPreparedData;
    }
}
