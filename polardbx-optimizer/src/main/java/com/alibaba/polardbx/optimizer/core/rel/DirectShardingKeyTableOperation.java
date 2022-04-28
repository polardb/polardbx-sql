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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 分片键点查操作
 */
public class DirectShardingKeyTableOperation extends BaseTableOperation {

    protected List<String> logicalTableNames;
    protected List<String> tableNames;

    private ShardProcessor shardProcessor;

    public DirectShardingKeyTableOperation(LogicalView logicalPlan, RelDataType rowType,
                                           String tableName, String sqlTemplate,
                                           List<Integer> paramIndex, ExecutionContext ec) {
        super(logicalPlan.getCluster(), logicalPlan.getTraitSet(), rowType, null, logicalPlan);

        this.tableNames = Collections.singletonList(tableName);
        this.dbIndex = null;
        this.sqlTemplate = sqlTemplate;
        this.paramIndex = paramIndex;
        this.logicalTableNames = this.tableNames;
        initShardProcessor(logicalPlan, tableName, ec);
    }

    private void initShardProcessor(LogicalView logicalView, String tableName, ExecutionContext ec) {
        this.shardProcessor = ShardProcessor.build(logicalView, tableName, ec);
    }

    /**
     * 避免拷贝Operation
     */
    public Pair<String, String> getDbIndexAndTableName(ExecutionContext ec) {
        return shardProcessor.shard(ec.getParams().getCurrentParameter(), ec);
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           ExecutionContext ec) {
        if (MapUtils.isEmpty(param) && CollectionUtils.isNotEmpty(paramIndex)) {
            throw new OptimizerException("Param list is empty.");
        }
        Pair<String, String> dbIndexAndTableName = ec.getDbIndexAndTableName();
        return new Pair<>(dbIndexAndTableName.getKey(),
            buildParam(dbIndexAndTableName.getValue(), param));
    }

    public DirectShardingKeyTableOperation(DirectShardingKeyTableOperation src) {
        super(src);
        tableNames = src.tableNames;
        logicalTableNames = src.logicalTableNames;
        shardProcessor = src.shardProcessor;
    }

    @Override
    public List<String> getTableNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected ExplainInfo buildExplainInfo(Map<Integer, ParameterContext> params, ExecutionContext executionContext) {
        if (MapUtils.isEmpty(params)) {
            return new ExplainInfo(tableNames, dbIndex, null);
        }
        Pair<String, String> dbIndexAndTableName = executionContext.getDbIndexAndTableName();
        String tableName = dbIndexAndTableName.getValue();
        return new ExplainInfo(Collections.singletonList(tableName), dbIndexAndTableName.getKey(),
            buildParam(tableName, params));
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        DirectShardingKeyTableOperation operation = new DirectShardingKeyTableOperation(this);
        return operation;
    }

    @Override
    protected String getExplainName() {
        return "DirectShardingKeyOperation";
    }
}
