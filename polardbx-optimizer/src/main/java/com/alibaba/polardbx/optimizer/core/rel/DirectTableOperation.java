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

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lingce.ldm 2017-11-20 11:22
 */
public class DirectTableOperation extends BaseTableOperation {

    private List<Map<Integer, ParameterContext>> batchParameters;
    private List<String> logicalTableNames; // log tables
    private List<String> tableNames; // phy tables

    public DirectTableOperation(RelNode logicalPlan, RelDataType rowType, List<String> logicalTableNames,
                                List<String> tableNames, String dbIndex,
                                BytesSql sqlTemplate, List<Integer> paramIndex) {
        super(logicalPlan.getCluster(), logicalPlan.getTraitSet(), rowType, null, logicalPlan);
        this.tableNames = tableNames;
        this.dbIndex = dbIndex;
        this.bytesSql = sqlTemplate;
        this.paramIndex = paramIndex;
        this.logicalTableNames = logicalTableNames;
    }

    public DirectTableOperation(DirectTableOperation src) {
        super(src);
        tableNames = src.tableNames;
        logicalTableNames = src.logicalTableNames;
    }

    /**
     * for ut test only
     */
    public DirectTableOperation(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet);
    }

    @Override
    public List<Map<Integer, ParameterContext>> getBatchParameters() {
        return batchParameters;
    }

    @Override
    public void setDbIndex(String dbIndex) {
        this.dbIndex = dbIndex;
    }

    @Override
    public List<String> getTableNames() {
        return tableNames;
    }

    @Override
    public List<String> getLogicalTableNames() {
        return this.logicalTableNames;
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           List<List<String>> phyTableNamesOutput,
                                                                           ExecutionContext executionContext) {
        if (phyTableNamesOutput != null) {
            for (String tableName : tableNames) {
                phyTableNamesOutput.add(ImmutableList.of(tableName));
            }
        }
        if (executionContext.isBatchPrepare()) {
            this.batchParameters = executionContext.getParams().getBatchParameters();
            return new Pair<>(dbIndex, null);
        }
        if (MapUtils.isEmpty(param) && CollectionUtils.isNotEmpty(paramIndex)) {
            throw new OptimizerException("Param list is empty.");
        }
        Pair<String, Map<Integer, ParameterContext>> result = new Pair<>(dbIndex, buildParam(param));
        return result;
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           ExecutionContext executionContext) {
        return getDbIndexAndParam(param, null, executionContext);
    }

    private Map<Integer, ParameterContext> buildParam(Map<Integer, ParameterContext> param) {
        Map<Integer, ParameterContext> newParam = new HashMap<>();
        int index = 1;
        for (int i : paramIndex) {
            newParam.put(index, PlannerUtils.changeParameterContextIndex(param.get(i + 1), index));
            index++;
        }
        return newParam;
    }

    @Override
    protected ExplainInfo buildExplainInfo(Map<Integer, ParameterContext> params, ExecutionContext executionContext) {
        if (MapUtils.isEmpty(params)) {
            return new ExplainInfo(tableNames, dbIndex, null);
        }
        return new ExplainInfo(tableNames, dbIndex, buildParam(params));
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        DirectTableOperation directTableOperation = new DirectTableOperation(this);
        return directTableOperation;
    }
}
