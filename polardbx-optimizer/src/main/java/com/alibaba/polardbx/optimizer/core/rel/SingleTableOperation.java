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
import com.alibaba.polardbx.common.jdbc.TableName;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.util.PartitionPlanExplainUtil;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.google.common.collect.ImmutableList;
import com.googlecode.protobuf.format.JsonFormat;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsJsonWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 仅查询单库单表的 TableScan 节点，或 Insert 单个值
 *
 * @author lingce.ldm 2017-11-15 18:14
 */
public class SingleTableOperation extends BaseTableOperation {
    protected final String logicalTableName;
    protected final ShardProcessor shardProcessor;

    // Only for INSERT. -1 means there's no auto increment column
    private final int autoIncParamIndex;

    public final static int NO_AUTO_INC = -1;

    public SingleTableOperation(RelNode logicalPlan, ShardProcessor shardProcessor, String logicalTableName,
                                BytesSql sqlTemplate,
                                List<Integer> paramIndex, int autoIncParamIndex) {
        super(logicalPlan.getCluster(), logicalPlan.getTraitSet(), logicalPlan.getRowType(), null, logicalPlan);
        this.shardProcessor = shardProcessor;
        this.logicalTableName = logicalTableName;
        this.bytesSql = sqlTemplate;
        this.paramIndex = paramIndex;
        this.autoIncParamIndex = autoIncParamIndex;
    }

    protected SingleTableOperation(SingleTableOperation singleTableOperation) {
        super(singleTableOperation);
        this.shardProcessor = singleTableOperation.shardProcessor;
        this.logicalTableName = singleTableOperation.logicalTableName;
        this.autoIncParamIndex = singleTableOperation.autoIncParamIndex;
    }

    @Override
    public void setDbIndex(String dbIndex) {
        this.dbIndex = dbIndex;
    }

    @Override
    public List<String> getTableNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getLogicalTableNames() {
        return ImmutableList.of(logicalTableName);
    }

    public int getAutoIncParamIndex() {
        return autoIncParamIndex;
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           List<List<String>> phyTableNamesOutput,
                                                                           ExecutionContext executionContext) {
        if (MapUtils.isEmpty(param) && CollectionUtils.isNotEmpty(paramIndex)) {
            if (logicalPlan instanceof LogicalView
                && executionContext.getSchemaManager(((LogicalView) logicalPlan).getSchemaName())
                .getTddlRuleManager().getPartitionInfoManager().isNewPartDbTable(logicalTableName)) {
                // pass for single partition table without param
            } else {
                throw new OptimizerException("Param list is empty.");
            }
        }
        // dbIndexAndTableName: Pair<GroupName, PhyTableName>
        Pair<String, String> dbIndexAndTableName = shardProcessor.shard(param, executionContext);
        final String value = dbIndexAndTableName.getValue();
        dbIndexAndTableName = new Pair<>(dbIndexAndTableName.getKey(), value);
        if (phyTableNamesOutput != null) {
            phyTableNamesOutput.add(ImmutableList.of(dbIndexAndTableName.getValue()));
        }
        return new Pair<>(dbIndexAndTableName.getKey(), buildParam(dbIndexAndTableName.getValue(), param));
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(
        Map<Integer, ParameterContext> param, ExecutionContext executionContext) {
        return getDbIndexAndParam(param, null, executionContext);
    }

    /**
     * Key : phyGroup
     * Val : phyTable
     */
    public Pair<String, String> getPhyGroupAndPhyTablePair(
        Map<Integer, ParameterContext> param, ExecutionContext executionContext) {
        return shardProcessor.shard(param, executionContext);
    }

//    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
//                                                                            List<String> phyTablesOutput,
//                                                                            ExecutionContext executionContext) {
//        if (MapUtils.isEmpty(param) && CollectionUtils.isNotEmpty(paramIndex)) {
//            if (logicalPlan instanceof LogicalView
//                && executionContext.getSchemaManager(((LogicalView) logicalPlan).getSchemaName())
//                .getTddlRuleManager().getPartitionInfoManager().isNewPartDbTable(logicalTableName)) {
//                // pass for single partition table without param
//            } else {
//                throw new OptimizerException("Param list is empty.");
//            }
//        }
//        // dbIndexAndTableName: Pair<GroupName, PhyTableName>
//        Pair<String, String> dbIndexAndTableName = shardProcessor.shard(param, executionContext);
//        final String value = dbIndexAndTableName.getValue();
//        dbIndexAndTableName = new Pair<>(dbIndexAndTableName.getKey(), value);
//        if (phyTablesOutput != null) {
//            phyTablesOutput.add(dbIndexAndTableName.getValue());
//        }
//        return new Pair<>(dbIndexAndTableName.getKey(), buildParam(dbIndexAndTableName.getValue(), param));
//    }

    @Override
    protected ExplainInfo buildExplainInfo(Map<Integer, ParameterContext> params, ExecutionContext executionContext) {
        if (MapUtils.isEmpty(params)) {
            boolean newPartDbTbl =
                PartitionPlanExplainUtil.checkIfNewPartDbTbl(this.schemaName, ImmutableList.of(logicalTableName), null,
                    executionContext);
            if (!newPartDbTbl) {
                /**
                 * When params is empty, its predicate may contain const literal instead of dynamic params, such as pk is null,
                 * so new partition table can perform pruning in this situation.
                 */
                return new ExplainInfo(ImmutableList.of(logicalTableName), StringUtils.EMPTY, null);
            }
        }
        final List<String> tables = new LinkedList<>();
        final List<List<String>> phyTableNames = new LinkedList<>();
        final Pair<String, Map<Integer, ParameterContext>> dbIndexAndParam =
            getDbIndexAndParam(params, phyTableNames, executionContext);
        tables.add(phyTableNames.get(0).get(0));
        return new ExplainInfo(tables, dbIndexAndParam.getKey(), dbIndexAndParam.getValue());
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        SingleTableOperation singleTableOperation = new SingleTableOperation(this);
        return singleTableOperation;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        ExecutionContext execCtx = (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext();
        ExplainInfo explainInfo = buildExplainInfo(((RelDrdsWriter) pw).getParams(), execCtx);
        pw.item(RelDrdsWriter.REL_NAME, getExplainName());
        boolean newPartDbTbl =
            PartitionPlanExplainUtil.checkIfNewPartDbTbl(this.schemaName, ImmutableList.of(logicalTableName), null,
                execCtx);
        String phyTableString = null;
        if (!newPartDbTbl) {
            String groupAndTableName = explainInfo.groupName + (TStringUtil.isNotBlank(explainInfo.groupName) ? "." : "")
                + StringUtils.join(explainInfo.tableNames, ",");
            phyTableString = groupAndTableName;
        } else {
            PartitionInfo partInfo =
                execCtx.getSchemaManager(this.schemaName).getTddlRuleManager().getPartitionInfoManager()
                    .getPartitionInfo(logicalTableName);
            String phyGrp = explainInfo.groupName;
            String phyTbl = (String) explainInfo.tableNames.get(0);
            String targetPartName = "";
            PartitionSpec ps = partInfo.getPartSpecSearcher().getPartSpec(phyGrp, phyTbl);
            if (ps != null) {
                targetPartName = ps.getName();
            }
            phyTableString = String.format("%s[%s]", logicalTableName, targetPartName);
        }

        pw.itemIf("tables", phyTableString, phyTableString != null);
        String sql = TStringUtil.replace(getNativeSql(), "\n", " ");
        pw.item("sql", sql);
        StringBuilder builder = new StringBuilder();
        if (MapUtils.isNotEmpty(explainInfo.params)) {
            String operator = "";
            for (Object c : explainInfo.params.values()) {
                Object v = ((ParameterContext) c).getValue();
                builder.append(operator);
                if (v instanceof TableName) {
                    builder.append(((TableName) v).getTableName());
                } else {
                    builder.append(v == null ? "NULL" : v.toString());
                }
                operator = ",";
            }
            pw.item("params", builder.toString());
        }

        // XPlan explain.
        final ExecutionContext executionContext;
        if (pw instanceof RelDrdsWriter) {
            executionContext = (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext();
        } else if (pw instanceof RelDrdsJsonWriter) {
            executionContext = (ExecutionContext) ((RelDrdsJsonWriter) pw).getExecutionContext();
        } else {
            executionContext = null;
        }
        if (XTemplate != null && executionContext != null &&
            executionContext.getParamManager().getBoolean(ConnectionParams.EXPLAIN_X_PLAN)) {
            final JsonFormat format = new JsonFormat();
            final PolarxExecPlan.ExecPlan plan = XTemplate.explain(executionContext);
            if (null == plan) {
                pw.item("XPlan", "Denied by param.");
            } else {
                pw.item("XPlan", format.printToString(plan));
            }
        }
        return pw;
    }
}
