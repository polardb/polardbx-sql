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

package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;

import java.util.List;
import java.util.Map;

public class AlterPartitionTableBuilder extends AlterTableBuilder {

    public AlterPartitionTableBuilder(DDL ddl, AlterTablePreparedData preparedData, ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
    }

    public AlterPartitionTableBuilder(DDL ddl, AlterTablePreparedData preparedData,
                                      LogicalAlterTable logicalAlterTable,
                                      ExecutionContext executionContext) {
        super(ddl, preparedData, logicalAlterTable, executionContext);
    }

    @Override
    public void buildTableRuleAndTopology() {
        partitionInfo = OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
            .getPartitionInfo(preparedData.getTableName());
        tableTopology = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(partitionInfo);
        buildAlterPartitionReferenceTableTopology();
    }

    @Override
    public void buildPhysicalPlans() {
        if (preparedData.getIsGsi()) {
            relDdl.setSqlNode(((SqlAlterTable) relDdl.getSqlNode()).removeAfterColumns());
        }

        buildSqlTemplate();
        buildPhysicalPlans(preparedData.getTableName());
        handleInstantAddColumn();
    }

    public void buildAlterPartitionReferenceTableTopology() {
        if (preparedData.getReferencedTables() != null) {
            for (String referencedTable : preparedData.getReferencedTables()) {
                final PartitionInfo refPartInfo =
                    OptimizerContext.getContext(preparedData.getSchemaName()).getPartitionInfoManager()
                        .getPartitionInfo(referencedTable);
                final Map<String, List<List<String>>> refTopo =
                    PartitionInfoUtil.buildTargetTablesFromPartitionInfo(refPartInfo);
                if (refPartInfo.isBroadcastTable()) {
                    final String phyTable =
                        refTopo.values().stream().map(l -> l.get(0).get(0)).findFirst().orElse(null);
                    assert phyTable != null;
                    for (Map.Entry<String, List<List<String>>> entry : tableTopology.entrySet()) {
                        for (List<String> l : entry.getValue()) {
                            l.add(phyTable);
                        }
                    }
                } else {
                    for (Map.Entry<String, List<List<String>>> entry : refTopo.entrySet()) {
                        final List<List<String>> partList = tableTopology.get(entry.getKey());
                        assert partList != null;
                        assert partList.size() == entry.getValue().size();
                        for (int i = 0; i < entry.getValue().size(); ++i) {
                            partList.get(i).addAll(entry.getValue().get(i));
                        }
                    }
                }
            }
        }
    }
}
