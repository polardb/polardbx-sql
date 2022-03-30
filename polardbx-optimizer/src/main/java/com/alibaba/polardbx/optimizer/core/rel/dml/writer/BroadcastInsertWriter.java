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
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertBuilder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder.PhyTableShardResult;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.BroadcastWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.mapping.Mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Writer for INSERT or REPLACE on broadcast table
 *
 * @author chenmo.cm
 */
public class BroadcastInsertWriter extends InsertWriter implements DistinctWriter, BroadcastWriter {

    protected final Mapping deduplicateMapping;
    private final TableRule tableRule;

    public BroadcastInsertWriter(RelOptTable targetTable,
                                 LogicalInsert logicalInsert,
                                 Mapping deduplicateMapping,
                                 TableRule tableRule) {
        super(targetTable, logicalInsert);
        this.deduplicateMapping = deduplicateMapping;
        this.tableRule = tableRule;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext executionContext) {
        if (!DbInfoManager.getInstance().isNewPartitionDb(this.schemaName)) {
            return getInputForShardingTables(executionContext);
        } else {
            return getInputForPartitioningTables(executionContext);
        }
    }

    public List<RelNode> getInputForPartitioningTables(ExecutionContext executionContext) {

        String schemaName = insert.getSchemaName();
        String logicTbName = insert.getLogicalTableName();
        Map<String, Set<String>> topology =
            executionContext.getSchemaManager(schemaName).getTable(logicTbName).getPartitionInfo().getTopology();
        // Build plan
        final List<PhyTableShardResult> shardResults = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
            shardResults.add(new PhyTableShardResult(entry.getKey(), entry.getValue().iterator().next(), null));
        }

        final PhyTableInsertSharder partitioner = new PhyTableInsertSharder(insert,
            executionContext.getParams(),
            SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));

        final PhyTableInsertBuilder phyPlanBuilder = new PhyTableInsertBuilder(partitioner.getSqlTemplate(),
            executionContext,
            insert,
            insert.getDbType(),
            insert.getSchemaName());

        return phyPlanBuilder.build(shardResults);

    }

    public List<RelNode> getInputForShardingTables(ExecutionContext executionContext) {

        final String physicalTableName = tableRule.getTbNamePattern();

        List<String> groupNames = HintUtil.allGroup(insert.getSchemaName());

        // May use jingwei to sync broadcast table
        final boolean enableMultiWriteOnBroadcast =
            executionContext.getParamManager().getBoolean(ConnectionParams.CHOOSE_BROADCAST_WRITE);
        if (!enableMultiWriteOnBroadcast) {
            groupNames = groupNames.subList(0, 1);
        }

        final List<PhyTableShardResult> shardResults = new ArrayList<>();
        for (String groupIndex : groupNames) {
            shardResults.add(new PhyTableShardResult(groupIndex, physicalTableName, null));
        }

        final PhyTableInsertSharder partitioner = new PhyTableInsertSharder(insert,
            executionContext.getParams(),
            SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));

        final PhyTableInsertBuilder phyPlanbuilder = new PhyTableInsertBuilder(partitioner.getSqlTemplate(),
            executionContext,
            insert,
            insert.getDbType(),
            insert.getSchemaName());

        return phyPlanbuilder.build(shardResults);

    }

    @Override
    public Mapping getGroupingMapping() {
        return deduplicateMapping;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext ec, Function<DistinctWriter, List<List<Object>>> rowGenerator) {
        // Deduplicate
        final List<List<Object>> values = rowGenerator.apply(this);
        if (null == values || values.isEmpty()) {
            return new ArrayList<>();
        }
        final List<Map<Integer, ParameterContext>> batchParams = BuildPlanUtils.buildInsertBatchParam(values);
        final Parameters parameterSettings = ec.getParams();
        parameterSettings.setBatchParams(batchParams);
        return getInput(ec);
    }

}
