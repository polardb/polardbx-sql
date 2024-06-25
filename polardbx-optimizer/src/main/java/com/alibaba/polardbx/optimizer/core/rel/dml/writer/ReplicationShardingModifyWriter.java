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
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableModifyBuilder;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.ReplicationWriter;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ReplicationShardingModifyWriter extends ShardingModifyWriter implements ReplicationWriter {

    protected final TableMeta tableMeta;

    public ReplicationShardingModifyWriter(RelOptTable targetTable,
                                           LogicalModify modify,
                                           List<ColumnMeta> skMetas,
                                           Mapping pkMapping,
                                           Mapping skMapping,
                                           Mapping updateSetMapping,
                                           Mapping groupingMapping,
                                           TableMeta tableMeta,
                                           boolean withoutPk) {
        super(targetTable, modify, skMetas, pkMapping, skMapping, updateSetMapping, groupingMapping, withoutPk);
        this.tableMeta = tableMeta;
    }

    @Override
    public TableMeta getTableMeta() {
        return tableMeta;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext ec, Function<DistinctWriter, List<List<Object>>> rowGenerator) {
        List<RelNode> primaryRelNodes = super.getInput(ec, rowGenerator);
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(tableMeta.getSchemaName());
        List<RelNode> replicateRelNodes;
        if (isNewPart) {
            replicateRelNodes = getReplicateInput(ec, rowGenerator);
        } else {
            replicateRelNodes = getInputForMoveDatabase(primaryRelNodes);
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

    private List<RelNode> getInputForMoveDatabase(List<RelNode> relNodes) {
        List<RelNode> moveTableRelNodes = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(relNodes)) {
            for (RelNode relNode : relNodes) {
                BaseTableOperation baseTableOperation = ((BaseTableOperation) relNode);
                String dbIndex = baseTableOperation.getDbIndex();
                boolean canWrite = (ComplexTaskPlanUtils.canWrite(tableMeta, dbIndex) && !ComplexTaskPlanUtils
                    .isDeleteOnly(tableMeta, dbIndex)) || (ComplexTaskPlanUtils
                    .isDeleteOnly(tableMeta, dbIndex) && ComplexTaskPlanUtils
                    .isModifyDML(baseTableOperation.getKind()));
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

    private List<RelNode> getReplicateInput(ExecutionContext ec,
                                            Function<DistinctWriter, List<List<Object>>> rowGenerator) {
        final RelOptTable targetTable = getTargetTable();
        final Pair<String, String> qn = RelUtils.getQualifiedTableName(targetTable);

        // Deduplicate
        final List<List<Object>> distinctRows = rowGenerator.apply(this);
        List<RelNode> replicatedRelNode = new ArrayList<>();

        if (distinctRows.isEmpty()) {
            return replicatedRelNode;
        }
        // targetDb: { targetTb: [{ rowIndex, [pk1, pk2] }] }
        final Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResult = BuildPlanUtils
            .buildResultForShardingTable(qn.left, qn.right, distinctRows, skMetas, skMapping, pkMapping, ec, true);

        PartitionInfo newPartitionInfo = tableMeta.getNewPartitionInfo();
        Map<String, Set<String>> replicateDbIndexAndPhycialTables = new HashMap<>();
        for (PartitionSpec partitionSpec : newPartitionInfo.getPartitionBy().getPhysicalPartitions()) {
            if (!partitionSpec.getLocation().isVisiable() && ComplexTaskPlanUtils
                .canWrite(tableMeta, partitionSpec.getName())) {
                PartitionLocation location = partitionSpec.getLocation();
                replicateDbIndexAndPhycialTables
                    .computeIfAbsent(location.getGroupKey().toUpperCase(), o -> new HashSet<>())
                    .add(location.getPhyTableName().toUpperCase());
            }
        }

        final Map<String, Map<String, List<Pair<Integer, List<Object>>>>> replicatedShardResult = new HashMap<>();

        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> entry : shardResult.entrySet()) {
            if (replicateDbIndexAndPhycialTables.containsKey(entry.getKey().toUpperCase())) {
                Set<String> physicalTables = replicateDbIndexAndPhycialTables.get(entry.getKey().toUpperCase());
                for (Map.Entry<String, List<Pair<Integer, List<Object>>>> e : entry.getValue().entrySet()) {
                    if (physicalTables.contains(e.getKey().toUpperCase())) {
                        replicatedShardResult.computeIfAbsent(entry.getKey(), b -> new HashMap<>())
                            .put(e.getKey(), e.getValue());
                    }
                }
            }
        }
        if (replicatedShardResult.isEmpty()) {
            return replicatedRelNode;
        }
        final PhyTableModifyBuilder builder = new PhyTableModifyBuilder();
        switch (getOperation()) {
        case UPDATE:
            replicatedRelNode.addAll(
                builder.buildUpdateWithPk(modify, distinctRows, updateSetMapping, qn, replicatedShardResult, ec));
            break;
        case DELETE:
            replicatedRelNode.addAll(builder.buildDelete(modify, qn, replicatedShardResult, ec, withoutPk));
            break;
        default:
            throw new AssertionError("Cannot handle operation " + getOperation().name());
        }
        return replicatedRelNode;
    }
}
