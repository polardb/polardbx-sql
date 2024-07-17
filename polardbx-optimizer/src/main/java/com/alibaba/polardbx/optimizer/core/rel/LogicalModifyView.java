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
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lingce.ldm 2018-01-31 13:48
 */
public class LogicalModifyView extends LogicalView {

    public LogicalModifyView(LogicalView logicalView) {
        super(logicalView);
        this.tableNames = logicalView.getTableNames();
        this.pushDownOpt = logicalView.getPushDownOpt().copy(this, logicalView.getPushedRelNode());
        this.finishShard = logicalView.getFinishShard();
        this.hints = logicalView.getHints();
        this.hintContext = logicalView.getHintContext();
    }

    @Override
    public RelToSqlConverter getConverter() {
        return super.getConverter();
    }

    @Override
    public List<RelNode> getInput(ExecutionContext executionContext) {
        return getInput(getSqlTemplate(executionContext), executionContext);
    }

    /**
     * merge operations in the same group, excluding the following conditions:
     * 1. explain does not support batch merge
     * 2. intra group parallelism and batch execution may cause deadlock
     */
    protected List<RelNode> mergeGroupNode(List<RelNode> subNodes, ExecutionContext executionContext) {
        if (executionContext.getExplain() != null || executionContext.getGroupParallelism() != 1) {
            return subNodes;
        }

        List<RelNode> relNodes = new ArrayList<>();
        Map<String, List<PhyTableOperation>> groupAndQcs = new HashMap<>();
        for (RelNode q : subNodes) {
            String groupName = ((PhyTableOperation) q).getDbIndex();
            List<PhyTableOperation> qcs = groupAndQcs.computeIfAbsent(groupName, k -> new ArrayList<>());
            qcs.add((PhyTableOperation) q);
        }
        for (List<PhyTableOperation> groupNode : groupAndQcs.values()) {
            if (groupNode.size() > 1) {
                List<List<String>> newTableNames = new ArrayList<>();
                List<Map<Integer, ParameterContext>> batchParameters = new ArrayList<>();
                for (PhyTableOperation queryOperation : groupNode) {
                    batchParameters.add(queryOperation.getParam());
                    queryOperation.setParam(null);
                    newTableNames.addAll(queryOperation.getTableNames());
                }
                // 不同分表以 batch param 形式执行
                PhyTableOperation firstPhyTableOp = groupNode.get(0);
                firstPhyTableOp.setBatchParameters(batchParameters);
                firstPhyTableOp.setTableNames(newTableNames);
                relNodes.add(firstPhyTableOp);
            } else {
                relNodes.add(groupNode.get(0));
            }
        }
        return relNodes;
    }

    @Override
    public List<RelNode> getInputWithoutCache(ExecutionContext executionContext) {
        return getInput(executionContext);
    }

    /**
     * getInput with sqlTemplate. Why not cache sqlTemplate: LogicalModifyView
     * is put into planCache, so sqlTemplate should be recreated.
     */
    public List<RelNode> getInput(SqlNode sqlTemplate, ExecutionContext executionContext) {
        List<Map<Integer, ParameterContext>> params;
        if (executionContext.getParams() == null) {
            params = Collections.singletonList(null);
        } else {
            params = executionContext.getParams().getBatchParameters();
        }
        List<RelNode> relNodes = new ArrayList<>();
        for (Map<Integer, ParameterContext> param : params) {
            if (executionContext.getParams() != null) {
                executionContext.getParams().setParams(param);
            }
            Map<String, List<List<String>>> targetTables;
            Map<com.alibaba.polardbx.common.utils.Pair<String, List<String>>, Parameters> pruningMap =
                OptimizerUtils.pruningInValue(this, executionContext);
            if (pruningMap == null) {
                targetTables = getTargetTables(executionContext);
            } else {
                targetTables = transformToTargetTables(pruningMap);
            }
            List<String> logTbls = new ArrayList<>();

            logTbls.addAll(this.tableNames);
            PhyTableModifyViewBuilder phyTableModifyBuilder = new PhyTableModifyViewBuilder(sqlTemplate,
                targetTables,
                param,
                this,
                dbType,
                logTbls, getSchemaName(),
                pruningMap);
            relNodes.addAll(phyTableModifyBuilder.build(executionContext));
        }
        if (relNodes.size() > 1 && relNodes.get(0) instanceof PhyTableOperation) {
            return mergeGroupNode(relNodes, executionContext);
        } else {
            return relNodes;
        }
    }

    @Override
    public String explainNodeName() {
        return "LogicalModifyView";
    }

    public TableModify getTableModify() {
        return this.pushDownOpt.getTableModify();
    }

    public boolean hasHint() {
        return targetTablesHintCache != null && !targetTablesHintCache.isEmpty();
    }

    @Override
    public LogicalModifyView copy(RelTraitSet traitSet) {
        LogicalModifyView logicalModifyView = new LogicalModifyView(this);
        logicalModifyView.traitSet = traitSet;
        logicalModifyView.pushDownOpt = pushDownOpt.copy(this, this.getPushedRelNode());
        return logicalModifyView;
    }
}
