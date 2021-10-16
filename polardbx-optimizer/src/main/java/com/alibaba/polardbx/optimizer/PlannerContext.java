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

package com.alibaba.polardbx.optimizer;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.PlannerContextWithParam;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class PlannerContext implements Context, PlannerContextWithParam {

    public static final PlannerContext EMPTY_CONTEXT = new PlannerContext(new ExecutionContext());

    private final AtomicInteger runtimeFilterId = new AtomicInteger(0);

    private String schemaName;

    private Map<String, Object> extraCmds = new HashMap<>();

    private ParamManager paramManager = new ParamManager(extraCmds);

    private Parameters params = new Parameters();

    private SqlKind sqlKind = SqlKind.NONE;

    private boolean needSPM = false;

    private WorkloadType workloadType = WorkloadType.TP;

    private RelOptCost cost = null;

    private boolean isExplain = false;

    /**
     * cache flag
     */
    private boolean isApply = false;

    private List<RelNode> cacheNodes = Lists.newArrayList();

    private BaselineInfo baselineInfo;

    private PlanInfo planInfo;

    private boolean isAutoCommit = true;

    private boolean shouldBuildScaleOutPlan = false;

    private String externalizePlan;

    private boolean isInSubquery = false;

    /**
     * whether to use heuristic reorder or not
     */
    private boolean shouldUseHeuOrder = false;

    private ExecutionContext executionContext;

    Function<RexNode, Object> evalFunc;

    private boolean isSkipPostOpt;

    private RelOptTable mysqlJoinReorderFirstTable = null;

    private int joinCount = -1;

    private long expectedRowcount = -1;

    private Map<LogicalTableScan, RexNode> exprMap;

    public <T> T unwrap(Class<T> clazz) {
        return clazz.isInstance(this) ? clazz.cast(this) : null;
    }

    public PlannerContext() {
        this.executionContext = new ExecutionContext();
    }

    public PlannerContext(ExecutionContext ec) {
        this.executionContext = ec;
        this.schemaName = executionContext.getSchemaName();
        this.extraCmds = executionContext.getExtraCmds();
        this.paramManager = new ParamManager(extraCmds);
        this.params = executionContext.getParams();
        this.isExplain = executionContext.getExplain() != null;
        this.isAutoCommit = executionContext.isAutoCommit();
    }

    public PlannerContext(ExecutionContext executionContext,
                          SqlKind sqlkind,
                          boolean isInSubquery) {
        this.executionContext = executionContext;
        this.schemaName = executionContext.getSchemaName();
        this.extraCmds = executionContext.getExtraCmds();
        this.paramManager = new ParamManager(extraCmds);
        this.params = executionContext.getParams().clone();
        this.isExplain = executionContext.getExplain() != null;
        this.isAutoCommit = executionContext.isAutoCommit();
        this.sqlKind = sqlkind;
        this.isInSubquery = isInSubquery;
    }

    protected PlannerContext(ExecutionContext executionContext,
                             Map<String, Object> extraCmds, Parameters params,
                             boolean isExplain,
                             boolean isAutoCommit,
                             SqlKind sqlkind,
                             boolean isInSubquery,
                             boolean shouldUseHeuOrder,
                             WorkloadType workloadType) {
        this.executionContext = executionContext;
        this.schemaName = executionContext.getSchemaName();
        this.extraCmds = extraCmds;
        this.paramManager = new ParamManager(extraCmds);
        this.params = params.clone();
        this.isExplain = isExplain;
        this.isAutoCommit = isAutoCommit;
        this.sqlKind = sqlkind;
        this.isInSubquery = isInSubquery;
        this.shouldUseHeuOrder = shouldUseHeuOrder;
        this.workloadType = workloadType;
    }

    public PlannerContext copyWithInSubquery() {
        PlannerContext ret = new PlannerContext(executionContext, extraCmds,
            params,
            isExplain,
            isAutoCommit,
            sqlKind,
            isInSubquery,
            shouldUseHeuOrder,
            workloadType);
        ret.isInSubquery = true;
        ret.joinCount = joinCount;
        return ret;
    }

    public static PlannerContext fromExecutionContext(ExecutionContext context) {
        return new PlannerContext(context,
            SqlKind.NONE,
            false
        );
    }

    public static PlannerContext getPlannerContext(RelOptRuleCall relOptRuleCall) {
        return getPlannerContext(relOptRuleCall.rels[0]);
    }

    public static PlannerContext getPlannerContext(RelNode relNode) {
        return getPlannerContext(relNode.getCluster());
    }

    public static PlannerContext getPlannerContext(RelOptCluster cluster) {
        Context context = cluster.getPlanner().getContext();
        if (context instanceof PlannerContext) {
            return context.unwrap(PlannerContext.class);
        } else {
            return EMPTY_CONTEXT;
        }
    }

    public static PlannerContext getPlannerContext(ExecutionContext ec, Function<RexNode, Object> evalFunc) {
        final PlannerContext result = new PlannerContext();
        result.setSchemaName(ec.getSchemaName());
        result.setExecutionContext(ec);
        result.setParams(ec.getParams().clone());
        result.setEvalFunc(evalFunc);

        return result;
    }

    @Override
    public Parameters getParams() {
        return params;
    }

    @Override
    public void setParams(Parameters params) {
        this.params = params;
    }

    public Map<String, Object> getExtraCmds() {
        return extraCmds;
    }

    public void setExtraCmds(Map<String, Object> extraCmds) {
        this.extraCmds = extraCmds;
        this.paramManager = new ParamManager(extraCmds);
    }

    public ParamManager getParamManager() {
        return paramManager;
    }

    public boolean isExplain() {
        return isExplain;
    }

    public void setExplain(boolean explain) {
        isExplain = explain;
    }

    public boolean isNeedSPM() {
        return needSPM;
    }

    public void enableSPM(boolean complexQuery) {
        this.needSPM = complexQuery;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    public void setSqlKind(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    public PlanInfo getPlanInfo() {
        return planInfo;
    }

    public void setPlanInfo(PlanInfo planInfo) {
        this.planInfo = planInfo;
    }

    public BaselineInfo getBaselineInfo() {
        return baselineInfo;
    }

    public void setBaselineInfo(BaselineInfo baselineInfo) {
        this.baselineInfo = baselineInfo;
    }

    public boolean isApply() {
        return isApply;
    }

    public PlannerContext setApply(boolean apply) {
        isApply = apply;
        return this;
    }

    /**
     * cache relnode
     */
    public List<RelNode> getCacheNodes() {
        return cacheNodes;
    }

    public boolean isAutoCommit() {
        return isAutoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        isAutoCommit = autoCommit;
    }

    public boolean isShouldBuildScaleOutPlan() {
        return shouldBuildScaleOutPlan;
    }

    public void setShouldBuildScaleOutPlan(boolean shouldBuildScaleOutPlan) {
        this.shouldBuildScaleOutPlan = shouldBuildScaleOutPlan;
    }

    public boolean isShouldUseHeuOrder() {
        return shouldUseHeuOrder;
    }

    public void setShouldUseHeuOrder(boolean shouldUseHeuOrder) {
        this.shouldUseHeuOrder = shouldUseHeuOrder;
    }

    public String getExternalizePlan() {
        return externalizePlan;
    }

    public void setExternalizePlan(String externalizePlan) {
        this.externalizePlan = externalizePlan;
    }

    public boolean isInSubquery() {
        return isInSubquery;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;
    }

    @Override
    public Function<RexNode, Object> getEvalFunc() {
        return evalFunc;
    }

    @Override
    public void setEvalFunc(Function<RexNode, Object> evalFunc) {
        this.evalFunc = evalFunc;
    }

    public boolean isSkipPostOpt() {
        return isSkipPostOpt;
    }

    public void setSkipPostOpt(boolean skipPostOpt) {
        isSkipPostOpt = skipPostOpt;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public WorkloadType getWorkloadType() {
        return workloadType;
    }

    public void setWorkloadType(WorkloadType workloadType) {
        this.workloadType = workloadType;
    }

    public RelOptCost getCost() {
        return cost;
    }

    public void setCost(RelOptCost cost) {
        this.cost = cost;
    }

    public int nextRuntimeFilterId() {
        return runtimeFilterId.getAndIncrement();
    }

    public RelOptTable getMysqlJoinReorderFirstTable() {
        return mysqlJoinReorderFirstTable;
    }

    public void setMysqlJoinReorderFirstTable(RelOptTable mysqlJoinReorderFirstTable) {
        this.mysqlJoinReorderFirstTable = mysqlJoinReorderFirstTable;
    }

    public int getJoinCount() {
        return joinCount;
    }

    public void setJoinCount(int joinCount) {
        this.joinCount = joinCount;
    }

    public Map<LogicalTableScan, RexNode> getExprMap() {
        return exprMap;
    }

    public void setExprMap(Map<LogicalTableScan, RexNode> exprMap) {
        this.exprMap = exprMap;
    }

    public long getExpectedRowcount() {
        return expectedRowcount;
    }

    public void setExpectedRowcount(long expectedRowcount) {
        this.expectedRowcount = expectedRowcount;
    }
}
