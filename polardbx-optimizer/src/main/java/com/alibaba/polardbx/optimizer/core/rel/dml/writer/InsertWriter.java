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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.EmptyShardProcessor;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertBuilder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.ShardProcessor;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Writer for INSERT or REPLACE on partitioned table
 * <p>
 * <pre>
 * For partitioned table, if every unique key(including primary key) satisfies that each partition keys of primary and
 * index table are also the index column of unique key, use this to send REPLACE to storage directly. Otherwise
 * send DELETE + INSERT instead.
 * </pre>
 *
 * @author chenmo.cm
 */
public class InsertWriter extends AbstractSingleWriter {
    protected final LogicalInsert insert;
    protected final String schemaName;
    protected final String tableName;
    protected final SingleTableInsert singleTableOperation;

    public InsertWriter(RelOptTable targetTable, LogicalInsert insert) {
        super(targetTable, insert.getOperation());
        this.insert = insert;
        this.singleTableOperation = buildSingleTableOperation(insert);
        this.schemaName = insert.getSchemaName();
        this.tableName = insert.getLogicalTableName();
    }

    public static SingleTableInsert buildSingleTableOperation(LogicalInsert insert) {
        final OptimizerContext oc = OptimizerContext.getContext(insert.getSchemaName());
        assert null != oc;

        // Hot key or unknown table?
        TddlRuleManager ruleManager = oc.getRuleManager();
        if (!ruleManager.checkTableExists(insert.getLogicalTableName())
            || ruleManager.containExtPartitions(insert.getLogicalTableName())) {
            return null;
        }

        final SqlNode sqlNode = insert.getSqlTemplate();
        final String sqlTemplate = RelUtils.toNativeSql(sqlNode, insert.getDbType());
        final List<Integer> paramIndex = PlannerUtils.getDynamicParamIndex(sqlNode);
        final ShardProcessor processor = ShardProcessor.buildSimpleShard(insert);
        return Optional.of(processor).filter(p -> !(p instanceof EmptyShardProcessor)).map(p -> {
            // Non prepare mode
            final SingleTableInsert result = new SingleTableInsert(insert,
                processor,
                insert.getLogicalTableName(),
                sqlTemplate,
                paramIndex,
                -1);
            result.setSchemaName(insert.getSchemaName());
            result.setKind(sqlNode.getKind());
            result.setNativeSqlNode(sqlNode);
            return result;
        }).orElse(null);
    }

    public LogicalInsert getInsert() {
        return insert;
    }

    public List<RelNode> getInput(ExecutionContext executionContext) {
        return getInput(executionContext, false);
    }

    public List<RelNode> getInput(ExecutionContext executionContext, boolean withoutSingleTableOptimize) {
        final LogicalDynamicValues input = RelUtils.getRelInput(insert);
        final Parameters paramRows = executionContext.getParams();

        final List<RelNode> result = new ArrayList<>();
        if (!withoutSingleTableOptimize && null != this.singleTableOperation && input.getTuples().size() == 1
            && !paramRows.isBatch()) {
            SingleTableInsert physicalPlan = new SingleTableInsert(this.singleTableOperation);
            Map<Integer, ParameterContext> params =
                executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter();
            Pair<String, Map<Integer, ParameterContext>> dbIndexAndParam =
                this.singleTableOperation.calcDbIndexAndParam(params, executionContext);
            String dbIndex = dbIndexAndParam.getKey();
            physicalPlan.setDbIndex(dbIndex);
            physicalPlan.setParam(params);
            result.add(physicalPlan);
        } else {

            final PhyTableInsertSharder insertPartitioner = new PhyTableInsertSharder(insert, paramRows,
                SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));

            final List<PhyTableInsertSharder.PhyTableShardResult> shardResults = new ArrayList<>(
                insertPartitioner
                    .shardValues(insert.getInput(), insert.getLogicalTableName(), executionContext));

            final PhyTableInsertBuilder phyTableInsertbuilder =
                new PhyTableInsertBuilder(insertPartitioner.getSqlTemplate(),
                    executionContext,
                    insert,
                    insert.getDbType(),
                    insert.getSchemaName());

            result.addAll(phyTableInsertbuilder.build(shardResults));
        }

        return result;
    }

    @Override
    public List<Writer> getInputs() {
        return Collections.emptyList();
    }

    public List<PhyTableInsertSharder.PhyTableShardResult> getShardResults(ExecutionContext executionContext) {
        PhyTableInsertSharder insertPartitioner = new PhyTableInsertSharder(insert,
            executionContext.getParams(),
            SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));
        List<PhyTableInsertSharder.PhyTableShardResult> shardResults = new ArrayList<>(
            insertPartitioner.shardValues(insert.getInput(), insert.getLogicalTableName(), executionContext));
        return shardResults;
    }

    public List<RelNode> getInputByShardResults(ExecutionContext executionContext,
                                                List<PhyTableInsertSharder.PhyTableShardResult> shardResults) {
        final PhyTableInsertSharder insertPartitioner = new PhyTableInsertSharder(insert,
            executionContext.getParams(),
            SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));
        PhyTableInsertBuilder phyTableInsertbuilder = new PhyTableInsertBuilder(insertPartitioner.getSqlTemplate(),
            executionContext,
            insert,
            insert.getDbType(),
            insert.getSchemaName());
        final List<RelNode> result = phyTableInsertbuilder.build(shardResults);
        return result;
    }
}
