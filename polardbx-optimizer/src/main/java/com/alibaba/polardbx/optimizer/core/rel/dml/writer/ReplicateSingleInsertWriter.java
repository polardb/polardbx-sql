package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertBuilder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder.PhyTableShardResult;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableInsert;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.dml.ReplicationWriter;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Writer for INSERT or REPLACE on single table
 *
 * @author luoyanxin
 */
public class ReplicateSingleInsertWriter extends SingleInsertWriter implements ReplicationWriter {

    public ReplicateSingleInsertWriter(RelOptTable targetTable,
                                       LogicalInsert insert,
                                       TableMeta tableMeta,
                                       TableRule tableRule) {
        super(targetTable, insert, tableMeta, tableRule);
    }

    @Override
    public List<RelNode> getInput(ExecutionContext executionContext) {
        List<RelNode> primaryRelNodes = super.getInput(executionContext);
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(tableMeta.getSchemaName());
        List<RelNode> replicateRelNodes;
        if (isNewPart) {
            replicateRelNodes = getReplicateInput(primaryRelNodes, executionContext);

            for (RelNode relNode : replicateRelNodes) {
                ((BaseQueryOperation) relNode).setReplicateRelNode(true);
                logReplicateSql(tableMeta, ((BaseTableOperation) relNode).getDbIndex(),
                    (BaseQueryOperation) relNode,
                    executionContext);
            }
            primaryRelNodes.addAll(replicateRelNodes);
        }
        return primaryRelNodes;
    }

    List<RelNode> getReplicateInput(List<RelNode> primaryRelNode, ExecutionContext executionContext) {
        List<RelNode> replicateRels = new ArrayList<>();
        if (GeneralUtil.isEmpty(primaryRelNode)) {
            return replicateRels;
        }
        assert tableMeta.getPartitionInfo().isSingleTable();
        PartitionSpec partitionSpec = tableMeta.getNewPartitionInfo().getPartitionBy().getPartitions().get(0);
        if (!partitionSpec.getLocation().isVisiable() && ComplexTaskPlanUtils
            .canWrite(tableMeta, partitionSpec.getName()) && !ComplexTaskPlanUtils
            .isDeleteOnly(tableMeta, partitionSpec.getName())) {
            PartitionLocation location = partitionSpec.getLocation();
            String targetDbIndex = location.getGroupKey();
            String targetphyTb = location.getPhyTableName();
            for (RelNode relNode:primaryRelNode) {
                RelNode replicateRel;
                if (relNode instanceof SingleTableOperation) {
                    assert primaryRelNode.size() == 1;
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
                    replicateRel = phyTableModifys.get(0);
                    ((PhyTableOperation) replicateRel).setDbIndex(targetDbIndex);

                    replicateRels.add(replicateRel);
                } else {
                    replicateRel =
                        relNode.copy(relNode.getTraitSet(), relNode.getInputs());
                    ((BaseTableOperation) replicateRel).setDbIndex(targetDbIndex);
                    replicateRels.add(replicateRel);
                }
            }
        }
        return replicateRels;
    }
    @Override
    public TableMeta getTableMeta() {
        return tableMeta;
    }
}
