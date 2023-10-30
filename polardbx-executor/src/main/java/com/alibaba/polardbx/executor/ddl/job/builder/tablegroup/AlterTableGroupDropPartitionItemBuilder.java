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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import org.apache.calcite.rel.core.DDL;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AlterTableGroupDropPartitionItemBuilder extends AlterTableGroupItemBuilder {

    public AlterTableGroupDropPartitionItemBuilder(DDL ddl,
                                                   AlterTableGroupItemPreparedData preparedData,
                                                   ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    @Override
    public Map<String, Set<String>> getSourcePhyTables() {
        if (GeneralUtil.isEmpty(sourcePhyTables) && GeneralUtil.isNotEmpty(
            preparedData.getInvisiblePartitionGroups())) {

            PartitionInfo partitionInfo =
                OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                    .getPartitionInfo(preparedData.getTableName());

            PartitionByDefinition partByDef = partitionInfo.getPartitionBy();
            PartitionByDefinition subPartByDef = partByDef.getSubPartitionBy();

            for (PartitionGroupRecord neighborPartition : preparedData.getInvisiblePartitionGroups()) {
                for (PartitionSpec partSpec : partByDef.getPartitions()) {
                    if (subPartByDef != null) {
                        for (PartitionSpec subPartSpec : partSpec.getSubPartitions()) {
                            if (subPartSpec.getName().equalsIgnoreCase(neighborPartition.getPartition_name())) {
                                PartitionLocation location = subPartSpec.getLocation();
                                sourcePhyTables.computeIfAbsent(location.getGroupKey(), o -> new HashSet<>())
                                    .add(location.getPhyTableName());
                                break;
                            }
                        }
                    } else {
                        if (partSpec.getName().equalsIgnoreCase(neighborPartition.getPartition_name())) {
                            PartitionLocation location = partSpec.getLocation();
                            sourcePhyTables.computeIfAbsent(location.getGroupKey(), o -> new HashSet<>())
                                .add(location.getPhyTableName());
                            break;
                        }
                    }
                }
            }
        }
        return sourcePhyTables;
    }
}
