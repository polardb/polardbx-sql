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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticTrace;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.PlannerContextWithParam;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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

    /**
     * restrict the times of CBOPushJoinRule
     */
    private boolean restrictCboPushJoin = false;
    private int pushJoinHitCount = 0;
    private Set<String> tablesInLV;

    private boolean gsiPrune = false;

    /**
     * If true, this plan can be optimize by adding FORCE INDEX PRIMARY for TSO trx, (but we do not optimize it.)
     * Set after building plan.
     */
    private boolean canOptByForcePrimary = false;

    /**
     * If true, we actually add FORCE INDEX PRIMARY when generating this plan.
     * Set before building plan.
     */
    private boolean addForcePrimary = false;

    private boolean hasRecursiveCte = false;
    /**
     * statistic trace
     */
    private boolean isNeedStatisticTrace = false;
    private List<StatisticTrace> statisticTraces = null;

    /**
     * record
     */
    private boolean enableSelectStatistics = false;

    private Map<String, Set<String>> viewMap = null;
    private Set<Integer> constantParamIndex = null;

    /**
     * enable the rule counter in cbo
     * there is no need to copy the value
     */
    private boolean enableRuleCounter = false;
    /**
     * record the times of rules called in cbo
     * there is no need to copy the value
     */
    private long ruleCount = 0;

    private boolean hasConstantFold = false;

    private boolean useColumnarPlanCache = false;

    private boolean inExprToLookupJoin = false;

    private int columnarMaxShardCnt = 20;

    private boolean useColumnar = false;

    private boolean localIndexHint = false;

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
        if (executionContext.getParams() == null) {
            this.params = new Parameters();
        } else {
            this.params = executionContext.getParams().clone();
        }
        this.isExplain = executionContext.getExplain() != null;
        this.isAutoCommit = executionContext.isAutoCommit();

        this.addForcePrimary = executionContext.isTsoTransaction() && executionContext.enableForcePrimaryForTso();
    }

    public PlannerContext(ExecutionContext executionContext,
                          SqlKind sqlkind,
                          boolean isInSubquery) {
        this.executionContext = executionContext;
        this.schemaName = executionContext.getSchemaName();
        this.extraCmds = executionContext.getExtraCmds();
        this.paramManager = new ParamManager(extraCmds);
        if (executionContext.getParams() == null) {
            this.params = new Parameters();
        } else {
            this.params = executionContext.getParams().clone();
        }
        this.isExplain = executionContext.getExplain() != null;
        this.isAutoCommit = executionContext.isAutoCommit();
        this.sqlKind = sqlkind;
        this.isInSubquery = isInSubquery;

        this.addForcePrimary = executionContext.isTsoTransaction() && executionContext.enableForcePrimaryForTso();
        this.useColumnarPlanCache = executionContext.isColumnarPlanCache();
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
        this.useColumnarPlanCache = executionContext.isColumnarPlanCache();
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
        this.params = params.clone();
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

    public void setEvalFuncFromExecutionContext() {
        if (executionContext != null) {
            this.evalFunc = RexUtils.getEvalFunc(executionContext);
        }
    }

    @Override
    public Optional<CalcitePlanOptimizerTrace> getCalcitePlanOptimizerTrace() {
        return executionContext == null ? Optional.empty() : executionContext.getCalcitePlanOptimizerTrace();
    }

    @Override
    public Object getExecContext() {
        return executionContext;
    }

    /**
     * Encodes context-specific extended parameters into a JSON string.
     *
     * @return A JSON-formatted string representation of the parameters.
     */
    public String encodeExtendedParametersToJson() {
        // Instantiate a builder object for constructing JSON content
        final JsonBuilder jsonBuilder = new JsonBuilder();

        // Create a map to hold parameters to be encoded
        Map<String, Object> extendedParams = new HashMap<>();

        // Add parameters to the map
        extendedParams.put("useColumnar", this.isUseColumnar());
        extendedParams.put("columnarMaxShardCnt", this.getColumnarMaxShardCnt());

        try {
            // Convert the map to a JSON string using the JsonBuilder
            return jsonBuilder.toJsonString(extendedParams);
        } catch (Exception e) {
            // Handle any potential exceptions during conversion
            ModuleLogInfo.getInstance().logRecord(Module.SPM, LogPattern.UNEXPECTED,
                new String[] {"encoding arguments to JSON", e.getMessage()}, LogLevel.CRITICAL);
            throw new RuntimeException(e);
        }
    }

    /**
     * Parses an extension argument string to configure query context settings related to columnar storage.
     *
     * @param extend The extension argument string containing configuration information for columnar storage.
     */
    public void decodeArguments(String extend) {
        // If the extension parameter is null or has zero length, simply return without further processing.
        if (extend == null || extend.isEmpty()) {
            return;
        }

        try {
            // Parse the extension argument string into a Map object.
            Map<String, Object> extendMap = JSON.parseObject(extend, HashMap.class);

            // Set whether to use columnar storage, defaulting to false.
            this.useColumnar = (Boolean) extendMap.getOrDefault("useColumnar", false);

            // Set the maximum number of shards for columnar storage, defaulting to 20
            this.columnarMaxShardCnt = (Integer) extendMap.getOrDefault("columnarMaxShardCnt", 20);
        } catch (Exception e) {
            // Handle any potential exceptions during conversion
            ModuleLogInfo.getInstance().logRecord(Module.SPM, LogPattern.UNEXPECTED,
                new String[] {"decoding arguments", e.getMessage()}, LogLevel.CRITICAL);
            throw new RuntimeException(e);
        }
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

    public boolean getRestrictCboPushJoin() {
        return restrictCboPushJoin;
    }

    public void setRestrictCboPushJoin(boolean restrictCboPushJoin) {
        this.restrictCboPushJoin = restrictCboPushJoin;
        if (restrictCboPushJoin) {
            tablesInLV = new HashSet<>();
        } else {
            tablesInLV = null;
        }
        pushJoinHitCount = 0;
    }

    /**
     * prune CBOPushJoinRule rule
     *
     * @param tables the list of tables in the logicalView
     * @return false if CBOPushJoinRule is invoked too many times and the table multiset has been optimized
     */
    public boolean addTableList(String tables) {
        pushJoinHitCount++;
        // enable when the join push rule is called too many times
        int limit = paramManager.getInt(ConnectionParams.CBO_RESTRICT_PUSH_JOIN_COUNT);
        if (pushJoinHitCount >= limit) {
            pushJoinHitCount = limit;
            return tablesInLV.add(tables);
        }
        return true;
    }

    public boolean isGsiPrune() {
        return gsiPrune;
    }

    public void setGsiPrune(boolean gsiPrune) {
        this.gsiPrune = gsiPrune;
    }

    public boolean isCanOptByForcePrimary() {
        return canOptByForcePrimary;
    }

    public boolean isAddForcePrimary() {
        return addForcePrimary;
    }

    public void setCanOptByForcePrimary(boolean canOptByForcePrimary) {
        this.canOptByForcePrimary = canOptByForcePrimary;
    }

    public void setAddForcePrimary(boolean addForcePrimary) {
        this.addForcePrimary = addForcePrimary;
    }

    public boolean isEnableSelectStatistics() {
        return enableSelectStatistics;
    }

    public void setEnableSelectStatistics(boolean enableSelectStatistics) {
        this.enableSelectStatistics = enableSelectStatistics;
        this.viewMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }

    public void addView(String schema, String view) {
        if (!this.enableSelectStatistics) {
            return;
        }
        this.viewMap.computeIfAbsent(schema, x -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER));
        this.viewMap.get(schema).add(view);
    }

    public Map<String, Set<String>> getViewMap() {
        return viewMap;
    }

    public boolean isNeedStatisticTrace() {
        return isNeedStatisticTrace;
    }

    public void setNeedStatisticTrace(boolean needStatisticTrace) {
        isNeedStatisticTrace = needStatisticTrace;
    }

    /**
     * recode statistic trace info into planner context
     */
    public void recordStatisticTrace(StatisticTrace trace) {
        if (statisticTraces == null) {
            statisticTraces = Lists.newArrayList();
        }
        statisticTraces.add(trace);
    }

    public void clearStatisticTraceInfo() {
        if (statisticTraces != null) {
            statisticTraces.clear();
        }
    }

    /**
     * transform statistic trace info from Map to string
     */
    public String formatStatisticTrace() {
        if (statisticTraces == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder("STATISTIC TRACE INFO:\n");

        // merge same trace
        Map<String, Integer> traceMap = Maps.newHashMap();

        for (StatisticTrace statisticTrace : statisticTraces) {
            traceMap.merge(statisticTrace.print(), 1, (a, b) -> a + b);
        }
        for (Map.Entry<String, Integer> e : traceMap.entrySet()) {
            if (e.getValue() != 1) {
                sb.append("MULTI[" + e.getValue() + "]").append("\n");
            }
            sb.append(e.getKey()).append("\n");
        }

        return sb.toString();
    }

    public boolean isEnableRuleCounter() {
        return enableRuleCounter;
    }

    public void setEnableRuleCounter(boolean enableRuleCounter) {
        this.enableRuleCounter = enableRuleCounter;
    }

    public void setRuleCount(long ruleCount) {
        this.ruleCount = ruleCount;
    }

    public long getRuleCount() {
        return ruleCount;
    }

    /**
     * is plan contains recursive cte
     */
    public boolean isHasRecursiveCte() {
        return hasRecursiveCte;
    }

    public void setHasRecursiveCte(boolean hasRecursiveCte) {
        this.hasRecursiveCte = hasRecursiveCte;
    }

    public int getColumnarMaxShardCnt() {
        return columnarMaxShardCnt;
    }

    public void setColumnarMaxShardCnt(int columnarMaxShardCnt) {
        if (columnarMaxShardCnt > 0) {
            this.columnarMaxShardCnt = columnarMaxShardCnt;
        }
    }

    public boolean isUseColumnar() {
        return useColumnar;
    }

    public void setUseColumnar(boolean useColumnar) {
        this.useColumnar = useColumnar;
    }

    public boolean isUseColumnarPlanCache() {
        return useColumnarPlanCache;
    }

    public Set<Integer> getConstantParamIndex() {
        return constantParamIndex;
    }

    public void setConstantParamIndex(Set<Integer> constantParamIndex) {
        this.constantParamIndex = constantParamIndex;
    }

    public boolean isInExprToLookupJoin() {
        return inExprToLookupJoin;
    }

    public void setInExprToLookupJoin(boolean inExprToLookupJoin) {
        this.inExprToLookupJoin = inExprToLookupJoin;
    }

    public boolean isHasConstantFold() {
        return hasConstantFold;
    }

    public void setHasConstantFold(boolean hasConstantFold) {
        this.hasConstantFold = hasConstantFold;
    }

    public boolean hasLocalIndexHint() {
        return localIndexHint;
    }

    public void setLocalIndexHint(boolean localIndexHint) {
        this.localIndexHint = localIndexHint;
    }
}
