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
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import com.googlecode.protobuf.format.JsonFormat;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsJsonWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * All single tables come from the different DB in auto mode, Thus
 * DirectMultiDBTableOperation can represent the push-down operation.
 * <p>
 * In the transaction scenario, I am trying to support the ability of multi-table
 * pushdown for different logical databases. However, I still cannot solve it,
 * especially in the following test.
 */
public class DirectMultiDBTableOperation extends BaseTableOperation {

    private List<Map<Integer, ParameterContext>> batchParameters;
    private List<TableId> logicalTableNames;
    private List<TableId> physicalTableNames; // log tables
    private TreeSet<String> orderSchemas = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private Map<String, Set<String>> schemaPhysicalMapping;

    //for test
    private boolean simplify = false;

    public DirectMultiDBTableOperation(RelNode logicalPlan, RelDataType rowType,
                                       List<TableId> logicalTableNames,
                                       List<TableId> physicalTableNames,
                                       Map<String, Set<String>> schemaPhysicalMapping,
                                       BytesSql sqlTemplate, List<Integer> paramIndex,
                                       String currentSchema,
                                       boolean simplify) {
        super(logicalPlan.getCluster(), logicalPlan.getTraitSet(), rowType, null, logicalPlan);
        this.logicalTableNames = logicalTableNames;
        this.bytesSql = sqlTemplate;
        this.paramIndex = paramIndex;
        this.physicalTableNames = physicalTableNames;
        this.schemaPhysicalMapping = schemaPhysicalMapping;
        logicalTableNames.stream().forEach(t -> orderSchemas.add(t.getDbName()));
        //schemaName is useless here
        this.schemaName = currentSchema;
        this.simplify = simplify;
    }

    public DirectMultiDBTableOperation(DirectMultiDBTableOperation src) {
        super(src);
        logicalTableNames = src.logicalTableNames;
        physicalTableNames = src.physicalTableNames;
        orderSchemas = src.orderSchemas;
        schemaPhysicalMapping = src.schemaPhysicalMapping;
        simplify = src.simplify;
    }

    @Override
    public List<Map<Integer, ParameterContext>> getBatchParameters() {
        return batchParameters;
    }

    public List<TableId> getLogicalTables() {
        return logicalTableNames;
    }

    public List<TableId> getPhysicalTableNames() {
        return physicalTableNames;
    }

    /**
     * the schema which is used to create connection.
     */
    public String getBaseSchemaName(ExecutionContext context) {
        String ctxSchema = context.getSchemaName();
        Set<String> heldTransactionSchema = null;
        if (context.getTransaction() != null && context.getTransaction().getConnectionHolder() != null) {
            heldTransactionSchema = context.getTransaction().getConnectionHolder().getHeldSchemas();
        }
        //1 select schema from transaction.
        if (heldTransactionSchema != null) {
            for (String targetSchema : heldTransactionSchema) {
                if (orderSchemas.contains(targetSchema)) {
                    return targetSchema;
                }
            }
        }
        //2 select schema from ExecutionContext.
        if (ctxSchema != null && orderSchemas.contains(ctxSchema)) {
            return ctxSchema;
        }

        //3 select the first schema from orderSchemas.
        return orderSchemas.stream().findFirst().get();
    }

    public String getBaseDbIndex(ExecutionContext context) {
        String schema = getBaseSchemaName(context);
        String t = getLogicalTables(schema).get(0);
        TddlRuleManager or = OptimizerContext.getContext(schema).getRuleManager();
        TableRule tr = or.getTableRule(t);
        if (tr != null) {
            return tr.getDbNamePattern();
        } else {
            return or.getDefaultDbIndex(t);
        }
    }

    /**
     * the tables which is from the given schema.
     */
    public List<String> getLogicalTables(String schema) {
        List<String> logicalTables = new ArrayList<>();
        for (TableId tableId : logicalTableNames) {
            if (tableId.getDbName().equalsIgnoreCase(schema)) {
                logicalTables.add(tableId.getTableName());
            }
        }
        if (logicalTables.size() == 0) {
            throw new RuntimeException("Don't find the tables from " + schema);
        }
        return logicalTables;
    }

    /**
     * the phyiscal tables which is from the given schema.
     */
    public List<String> getPhysicalTables(String schema) {
        String physicalDB = schemaPhysicalMapping.get(schema).stream().findFirst().get();
        List<String> physicalTables = new ArrayList<>();
        for (TableId tableId : physicalTableNames) {
            if (tableId.getDbName().equalsIgnoreCase(physicalDB)) {
                physicalTables.add(tableId.getTableName());
            }
        }
        if (physicalTables.size() == 0) {
            throw new RuntimeException("Don't find the tables from " + schema);
        }
        return physicalTables;
    }

    /**
     * To prevent misuse, this method is prohibited. Please use getLogicalTables
     */
    @Override
    public List<String> getTableNames() {
        throw new UnsupportedOperationException();
    }

    /**
     * To prevent misuse, this method is prohibited. Please use getBaseSchemaName
     */
    @Override
    public String getSchemaName() {
        throw new UnsupportedOperationException();
    }

    /**
     * To prevent misuse, this method is prohibited. Please use getBaseSchemaName
     */
    @Override
    public void setDbIndex(String dbIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * To prevent misuse, this method is prohibited.
     */
    @Override
    public String getDbIndex() {
        throw new UnsupportedOperationException();
    }

    /**
     * To prevent misuse, this method is prohibited.
     */
    @Override
    public List<String> getLogicalTableNames() {
        throw new UnsupportedOperationException();
    }

    /**
     * To prevent misuse, this method is prohibited.
     */
    @Override
    protected ExplainInfo buildExplainInfo(Map<Integer, ParameterContext> params, ExecutionContext executionContext) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param phyTableNamesOutput return the physical tables of the base schema.
     */
    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           List<List<String>> phyTableNamesOutput,
                                                                           ExecutionContext executionContext) {
        String schemaName = getBaseSchemaName(executionContext);
        String t = getLogicalTables(schemaName).get(0);
        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        TableRule tr = or.getTableRule(t);
        String dbIndex;
        if (tr != null) {
            dbIndex = tr.getDbNamePattern();
        } else {
            dbIndex = or.getDefaultDbIndex(t);
        }

        if (phyTableNamesOutput != null) {
            for (String physicalDB : getPhysicalTables(schemaName)) {
                phyTableNamesOutput.add(ImmutableList.of(physicalDB));
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
    public RelWriter explainTermsForDisplay(RelWriter pw) {

        Map<Integer, ParameterContext> params = ((RelDrdsWriter) pw).getParams();
        if (!MapUtils.isEmpty(params)) {
            params = buildParam(params);
        }
        pw.item(RelDrdsWriter.REL_NAME, getExplainName());
        pw.item("tables", simplify ? logicalTableNames : physicalTableNames);
        String sql = TStringUtil.replace(getNativeSql(), "\n", " ");
        pw.item("sql", simplify ? "" : sql);
        StringBuilder builder = new StringBuilder();
        if (MapUtils.isNotEmpty(params)) {
            String operator = "";
            for (Object c : params.values()) {
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

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        DirectMultiDBTableOperation directTableOperation = new DirectMultiDBTableOperation(this);
        return directTableOperation;
    }
}
