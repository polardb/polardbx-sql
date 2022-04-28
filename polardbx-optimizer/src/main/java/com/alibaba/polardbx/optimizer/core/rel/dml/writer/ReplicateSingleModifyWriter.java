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

package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ReplicationWriter;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.mapping.Mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * UPDATE/DELETE on one single table with primary key specified when scaleout/scalein
 *
 * @author luoyanxin
 */
public class ReplicateSingleModifyWriter extends SingleModifyWriter implements ReplicationWriter {


    private final TableMeta tableMeta;
    public ReplicateSingleModifyWriter(RelOptTable targetTable, LogicalModify modify, Mapping pkMapping,
                                       Mapping updateSetMapping, Mapping groupingMapping, TableMeta tableMeta) {
        super(targetTable, modify, pkMapping, updateSetMapping, groupingMapping);
        this.tableMeta = tableMeta;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext ec, Function<DistinctWriter, List<List<Object>>> rowGenerator) {
        List<RelNode> primaryRelNodes = super.getInput(ec, rowGenerator);
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(tableMeta.getSchemaName());
        if (isNewPart) {
            List<RelNode> replicateRelNodes;
            replicateRelNodes = getReplicateInput(primaryRelNodes);

            for (RelNode relNode : replicateRelNodes) {
                ((BaseQueryOperation) relNode).setReplicateRelNode(true);
                logReplicateSql(tableMeta, ((BaseTableOperation) relNode).getDbIndex(),
                    (BaseQueryOperation) relNode,
                    ec);
            }
            primaryRelNodes.addAll(replicateRelNodes);
        }
        return primaryRelNodes;
    }

    private List<RelNode> getReplicateInput(List<RelNode> relNodes) {
        List<RelNode> replicateRelNodes = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(relNodes)) {

            assert tableMeta.getPartitionInfo().isSingleTable();
            PartitionSpec partitionSpec = tableMeta.getNewPartitionInfo().getPartitionBy().getPartitions().get(0);
            if (!partitionSpec.getLocation().isVisiable() && ComplexTaskPlanUtils
                .canWrite(tableMeta, partitionSpec.getName())) {
                for (RelNode relNode : relNodes) {
                    PartitionLocation location = partitionSpec.getLocation();
                    String targetDbIndex = location.getGroupKey();
                    RelNode replicateRel =
                        relNode.copy(relNode.getTraitSet(), relNode.getInputs());
                    ((BaseTableOperation) replicateRel).setDbIndex(targetDbIndex);
                    replicateRelNodes.add(replicateRel);
                }
            }

        }
        return replicateRelNodes;
    }

    @Override
    public TableMeta getTableMeta() {
        return tableMeta;
    }
}
