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

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertBuilder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableModifyBuilder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.dml.ReplicationWriter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ReplicationInsertWriter extends InsertWriter implements ReplicationWriter {

    protected final TableMeta tableMeta;

    public ReplicationInsertWriter(RelOptTable targetTable,
                                   LogicalInsert insert,
                                   TableMeta tableMeta) {
        super(targetTable, insert);
        this.tableMeta = tableMeta;
    }

    @Override
    public TableMeta getTableMeta() {
        return tableMeta;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext executionContext) {
        List<RelNode> primaryRelNodes = super.getInput(executionContext);
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(tableMeta.getSchemaName());
        List<RelNode> replicateRelNodes;
        if (isNewPart) {
            replicateRelNodes = getReplicateInput(executionContext);
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
                    RelNode moveTableRel;
                    if (relNode instanceof SingleTableOperation) {
                        assert relNodes.size() == 1;
                        final Parameters paramRows = executionContext.getParams();
                        final PhyTableInsertSharder insertPartitioner = new PhyTableInsertSharder(insert, paramRows,
                            SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));

                        final List<PhyTableInsertSharder.PhyTableShardResult> shardResults = new ArrayList<>(
                            insertPartitioner
                                .shardValues(insert.getInput(), insert.getLogicalTableName(), executionContext));

                        // why need build again?
                        // because just change the dbIndex in SingleTableOperation doesn't work
                        // when call getDbIndexAndParam, SingleTableOperation will get the result
                        // from calc the shard
                        final PhyTableInsertBuilder phyTableInsertbuilder =
                            new PhyTableInsertBuilder(insertPartitioner.getSqlTemplate(),
                                executionContext,
                                insert,
                                insert.getDbType(),
                                insert.getSchemaName());

                        List<RelNode> phyTableModifys = phyTableInsertbuilder.build(shardResults);
                        assert phyTableModifys.size() == 1;
                        moveTableRel = phyTableModifys.get(0);
                        ((PhyTableOperation) moveTableRel).setDbIndex(GroupInfoUtil.buildScaloutGroupName(dbIndex));

                        moveTableRelNodes.add(moveTableRel);
                    } else {
                        moveTableRel =
                            baseTableOperation.copy(baseTableOperation.getTraitSet(), baseTableOperation.getInputs());
                        ((BaseTableOperation) moveTableRel).setDbIndex(GroupInfoUtil.buildScaloutGroupName(dbIndex));
                        moveTableRelNodes.add(moveTableRel);
                    }
                }
            }
        }
        return moveTableRelNodes;
    }

    private List<RelNode> getReplicateInput(ExecutionContext executionContext) {
        final Parameters paramRows = executionContext.getParams();

        final List<RelNode> result = new ArrayList<>();
        final PhyTableInsertSharder insertPartitioner = new PhyTableInsertSharder(insert, paramRows,
            SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));

        final List<PhyTableInsertSharder.PhyTableShardResult> shardResults = new ArrayList<>(
            insertPartitioner
                .shardValues(insert.getInput(), insert.getLogicalTableName(), executionContext, true));
        PhyTableModifyBuilder.removeNonReplicateShardResult(shardResults, tableMeta);
        final PhyTableInsertBuilder phyTableInsertbuilder =
            new PhyTableInsertBuilder(insertPartitioner.getSqlTemplate(),
                executionContext,
                insert,
                insert.getDbType(),
                insert.getSchemaName());

        result.addAll(phyTableInsertbuilder.build(shardResults));

        return result;
    }
}
