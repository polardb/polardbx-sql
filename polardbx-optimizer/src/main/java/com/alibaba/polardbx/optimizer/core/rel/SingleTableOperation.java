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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 仅查询单库单表的 TableScan 节点，或 Insert 单个值
 *
 * @author lingce.ldm 2017-11-15 18:14
 */
public class SingleTableOperation extends BaseTableOperation {

    private final String tableName;
    private final ShardProcessor shardProcessor;

    // Only for INSERT. -1 means there's no auto increment column
    private final int autoIncParamIndex;

    public final static int NO_AUTO_INC = -1;

    public SingleTableOperation(RelNode logicalPlan, ShardProcessor shardProcessor, String tableName,
                                String sqlTemplate,
                                List<Integer> paramIndex, int autoIncParamIndex) {
        super(logicalPlan.getCluster(), logicalPlan.getTraitSet(), logicalPlan.getRowType(), null, logicalPlan);
        this.shardProcessor = shardProcessor;
        this.tableName = tableName;
        this.sqlTemplate = sqlTemplate;
        this.paramIndex = paramIndex;
        this.autoIncParamIndex = autoIncParamIndex;
    }

    protected SingleTableOperation(SingleTableOperation singleTableOperation) {
        super(singleTableOperation);
        this.shardProcessor = singleTableOperation.shardProcessor;
        this.tableName = singleTableOperation.tableName;
        this.autoIncParamIndex = singleTableOperation.autoIncParamIndex;
    }

    @Override
    public List<String> getTableNames() {
        return ImmutableList.of(tableName);
    }

    public int getAutoIncParamIndex() {
        return autoIncParamIndex;
    }

    /**
     * 这个类需要使用getInput统一抽象，暂时处理全链路压测
     */
//    @Override
//    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param) {
//        return getDbIndexAndParam(param, null);
//    }
    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(
        Map<Integer, ParameterContext> param, ExecutionContext executionContext) {
        return getDbIndexAndParam(param, new LinkedList<String>(), executionContext);
    }

    /**
     * Key : phyGroup
     * Val : phyTable
     */
    public Pair<String, String> getPhyGroupAndPhyTablePair(
        Map<Integer, ParameterContext> param, ExecutionContext executionContext) {
        return shardProcessor.shard(param, executionContext);
    }

    private Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                            List<String> tables,
                                                                            ExecutionContext executionContext) {
        if (MapUtils.isEmpty(param) && CollectionUtils.isNotEmpty(paramIndex)) {
            if (logicalPlan instanceof LogicalView
                && executionContext.getSchemaManager(((LogicalView) logicalPlan).getSchemaName())
                .getTddlRuleManager().getPartitionInfoManager().isNewPartDbTable(tableName)) {
                // pass for single partition table without param
            } else {
                throw new OptimizerException("Param list is empty.");
            }
        }
        Pair<String, String> dbIndexAndTableName = shardProcessor.shard(param, executionContext);
        final String value = dbIndexAndTableName.getValue();
        dbIndexAndTableName = new Pair<>(dbIndexAndTableName.getKey(), value);
        tables.add(dbIndexAndTableName.getValue());
        return new Pair<>(dbIndexAndTableName.getKey(), buildParam(dbIndexAndTableName.getValue(), param));
    }

    @Override
    protected ExplainInfo buildExplainInfo(Map<Integer, ParameterContext> params, ExecutionContext executionContext) {
        if (MapUtils.isEmpty(params)) {
            return new ExplainInfo(ImmutableList.of(tableName), StringUtils.EMPTY, null);
        }
        final List<String> tables = new LinkedList<>();
        final Pair<String, Map<Integer, ParameterContext>> dbIndexAndParam =
            getDbIndexAndParam(params, tables, executionContext);
        return new ExplainInfo(tables, dbIndexAndParam.getKey(), dbIndexAndParam.getValue());
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        SingleTableOperation singleTableOperation = new SingleTableOperation(this);
        return singleTableOperation;
    }
}
