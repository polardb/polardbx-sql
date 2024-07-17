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
import com.alibaba.polardbx.optimizer.core.rel.PhyTableModifyBuilder;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ReplicationWriter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.mapping.Mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ReplicateDistinctInsertWriter extends DistinctInsertWriter implements ReplicationWriter {

    final TableMeta tableMeta;

    public ReplicateDistinctInsertWriter(RelOptTable targetTable,
                                         LogicalInsert insert,
                                         Mapping deduplicateMapping,
                                         TableMeta tableMeta) {
        super(targetTable, insert, deduplicateMapping);
        this.tableMeta = tableMeta;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext ec, Function<DistinctWriter, List<List<Object>>> rowGenerator) {

        List<RelNode> primaryRelNodes = super.getInput(ec, rowGenerator);
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(tableMeta.getSchemaName());
        List<RelNode> replicateRelNodes;
        if (isNewPart) {
            // Deduplicate
            final List<List<Object>> distinctRows = rowGenerator.apply(this);
            replicateRelNodes = PhyTableModifyBuilder.buildInsert(insert, distinctRows, ec, true);
        } else {
            replicateRelNodes = getInputForMoveDatabase(primaryRelNodes, ec);
        }
        for (RelNode relNode : replicateRelNodes) {
            ((BaseQueryOperation) relNode).setReplicateRelNode(true);
            logReplicateSql(tableMeta, ((BaseTableOperation) relNode).getDbIndex(),
                (BaseQueryOperation) relNode,
                ec);
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
                }
            }
        }
        return moveTableRelNodes;
    }

    @Override
    public TableMeta getTableMeta() {
        return tableMeta;
    }
}
