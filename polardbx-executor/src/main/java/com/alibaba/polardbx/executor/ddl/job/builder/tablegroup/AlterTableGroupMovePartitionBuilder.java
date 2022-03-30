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

import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMovePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;

import java.util.ArrayList;
import java.util.List;

public class AlterTableGroupMovePartitionBuilder extends AlterTableGroupBaseBuilder {

    public AlterTableGroupMovePartitionBuilder(DDL ddl, AlterTableGroupMovePartitionPreparedData preparedData,
                                               ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public List<String> getNewPhyTables(String tableName) {
        List<String> newPhyTables = new ArrayList<>();
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                .getPartitionInfo(tableName);
        List<PartitionSpec> partitionSpecs = partitionInfo.getPartitionBy().getPartitions();
        for (PartitionGroupRecord partitionGroupRecord : preparedData.getInvisiblePartitionGroups()) {
            PartitionSpec partitionSpec =
                partitionSpecs.stream().filter(o -> o.getName().equalsIgnoreCase(partitionGroupRecord.partition_name))
                    .findFirst().orElse(null);
            assert partitionSpec != null;
            newPhyTables.add(partitionSpec.getLocation().getPhyTableName());
        }
        return newPhyTables;
    }
}
