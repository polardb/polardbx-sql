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
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.ReplicationWriter;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.mapping.Mapping;

import java.util.ArrayList;
import java.util.List;

/**
 * Writer for INSERT or REPLACE on broadcast table
 *
 * @author chenmo.cm
 */
public class ReplicateBroadcastInsertWriter extends BroadcastInsertWriter implements ReplicationWriter {

    protected final TableMeta tableMeta;

    public ReplicateBroadcastInsertWriter(RelOptTable targetTable,
                                          LogicalInsert logicalInsert,
                                          Mapping deduplicateMapping,
                                          TableRule tableRule,
                                          TableMeta tableMeta) {
        super(targetTable, logicalInsert, deduplicateMapping, tableRule);
        this.tableMeta = tableMeta;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext executionContext) {
        List<RelNode> primaryRelNodes = super.getInput(executionContext);
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(tableMeta.getSchemaName());
        List<RelNode> replicateRelNodes;
        if (isNewPart) {
            replicateRelNodes = getReplicateInput(primaryRelNodes, executionContext);
        } else {
            replicateRelNodes = getInputForMoveDatabase(primaryRelNodes, executionContext);
        }
        for (RelNode relNode : replicateRelNodes) {
            ((BaseQueryOperation) relNode).setReplicateRelNode(true);
            logReplicateSql(tableMeta, ((BaseTableOperation) relNode).getDbIndex(),
                (BaseQueryOperation) relNode,
                executionContext);
        }
        primaryRelNodes.addAll(replicateRelNodes);
        return primaryRelNodes;
    }

    private List<RelNode> getInputForMoveDatabase(List<RelNode> relNodes, ExecutionContext executionContext) {
        List<RelNode> moveTableRelNodes = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(relNodes)) {
            for (RelNode relNode : relNodes) {
                BaseTableOperation baseTableOperation = ((BaseTableOperation) relNode);
                String dbIndex = baseTableOperation.getDbIndex();
                boolean canWrite = ComplexTaskPlanUtils.canWrite(tableMeta, dbIndex) && !ComplexTaskPlanUtils
                    .isDeleteOnly(tableMeta, dbIndex);
                if (canWrite) {
                    RelNode moveTableRel =
                        baseTableOperation.copy(baseTableOperation.getTraitSet(), baseTableOperation.getInputs());
                    ((BaseTableOperation) moveTableRel).setDbIndex(GroupInfoUtil.buildScaleOutGroupName(dbIndex));
                    moveTableRelNodes.add(moveTableRel);
                    logReplicateSql(tableMeta, ((BaseTableOperation) moveTableRel).getDbIndex(),
                        (BaseQueryOperation) moveTableRel,
                        executionContext);
                }
            }
        }
        return moveTableRelNodes;
    }

    private List<RelNode> getReplicateInput(List<RelNode> relNodes, ExecutionContext executionContext) {
        final List<RelNode> result = new ArrayList<>();
        if (GeneralUtil.isEmpty(relNodes)) {
            return result;
        }
        BaseTableOperation baseTableOperation = (BaseTableOperation) relNodes.get(0);
        for (PartitionSpec partitionSpec : tableMeta.getNewPartitionInfo().getPartitionBy().getPhysicalPartitions()) {
            if (!partitionSpec.getLocation().isVisiable() && ComplexTaskPlanUtils
                .canWrite(tableMeta, partitionSpec.getName()) && !ComplexTaskPlanUtils
                .isDeleteOnly(tableMeta, partitionSpec.getName())) {
                RelNode replicateRel =
                    baseTableOperation.copy(baseTableOperation.getTraitSet(), baseTableOperation.getInputs());
                ((BaseTableOperation) replicateRel).setDbIndex(partitionSpec.getLocation().getGroupKey());
                result.add(replicateRel);
            }
        }
        return result;
    }

    @Override
    public TableMeta getTableMeta() {
        return tableMeta;
    }
}
