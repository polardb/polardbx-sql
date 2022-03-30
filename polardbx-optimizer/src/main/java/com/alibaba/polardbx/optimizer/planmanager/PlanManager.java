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

package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilter;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlaceHolderExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.PostPlanner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.parametric.ParametricQueryAdvisor;
import com.alibaba.polardbx.optimizer.planmanager.parametric.Point;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionParams.MINOR_TOLERANCE_VALUE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.SPM_RECENTLY_EXECUTED_PERIOD;
import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManager.PLAN_SOURCE.PLAN_CACHE;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManager.PLAN_SOURCE.SPM_FIX;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManager.PLAN_SOURCE.SPM_NEW_BUILD;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManager.PLAN_SOURCE.SPM_PQO;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.getColumnsFromPlan;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.getRexNodeTableMap;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.loggerSpm;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.mergeColumns;
import static org.apache.calcite.util.Litmus.IGNORE;

public class PlanManager extends AbstractLifecycle implements BaselineManageable, PlanManageable {

    private static final Logger logger = LoggerFactory.getLogger(PlanManager.class);

    private static final int WORKLOAD_FEEDBACK_THRESHOLD = 3;

    public static final int ERROR_TABLES_HASH_CODE = -1;

    public static final double MINOR_TOLERANCE_RATIO = 0.6D;
    public static final double MAX_TOLERANCE_RATIO = 1.4D;

    private static final Random rand = new Random(System.currentTimeMillis());

    private long lastReadTime;

    // parameterizedSql -> BaselineInfo
    private Map<String, BaselineInfo> baselineMap = new ConcurrentHashMap<>();

    private final String schemaName;

    private SystemTableBaselineInfo systemTableBaselineInfo;

    private SystemTablePlanInfo systemTablePlanInfo;

    private IBaselineSyncController baselineSyncController;

    private ScheduledThreadPoolExecutor scheduler;

    private ThreadPoolExecutor executor;

    private PlanCache planCache;

    private BloomFilter sqlHistoryBloomfilter = BloomFilter.createEmpty(1000000, 0.05);

    private ParametricQueryAdvisor parametricQueryAdvisor;

    /**
     * TDataSource connection properties manager
     */
    private final ParamManager paramManager;

    private Map<BaselineInfo, Map<Integer, ParameterContext>> parametersCache = Maps.newConcurrentMap();

    public PlanManager(String schemaName,
                       SystemTableBaselineInfo systemTableBaselineInfo,
                       SystemTablePlanInfo systemTablePlanInfo,
                       IBaselineSyncController baselineSyncController,
                       PlanCache planCache,
                       ParametricQueryAdvisor parametricQueryAdvisor,
                       Map<String, Object> connectionProperties) {
        this.schemaName = schemaName;
        this.systemTableBaselineInfo = systemTableBaselineInfo;
        this.systemTablePlanInfo = systemTablePlanInfo;
        this.baselineSyncController = baselineSyncController;
        this.planCache = planCache;
        this.parametricQueryAdvisor = parametricQueryAdvisor;
        // Disable spm for mock mode
        if (ConfigDataMode.isFastMock()) {
            connectionProperties.put(ConnectionParams.ENABLE_SPM.getName(), false);
        }
        this.paramManager = new ParamManager(connectionProperties);
    }

    @Override
    protected void doInit() {
        if (ConfigDataMode.isFastMock()) {
            return;
        }
        this.scheduler = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "PlanManager scheduler");
                thread.setDaemon(true);
                return thread;
            }
        });
        this.executor = new ThreadPoolExecutor(
            1, 1, 1800, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(512),
            new NamedThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r, "PlanManager executor");
                    thread.setDaemon(true);
                    return thread;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy());
        long start = System.currentTimeMillis();
        if (ConfigDataMode.isMasterMode()) {
            systemTableBaselineInfo.createTableIfNotExist();
            systemTablePlanInfo.createTableIfNotExist();
        }
        loadBaseLineInfoAndPlanInfo();
        initParametricInfo();
        startReadForeverAsync();
        startCheckForeverAsync();
        startFlushForeverAsync();
        startCheckPlan();
        long end = System.currentTimeMillis();
        logger.warn("PlanManager init consuming " + (end - start) / 1000.0 + " seconds");
    }

    private void initParametricInfo() {
        Map<String, Set<Point>> pointMap = Maps.newHashMap();
        for (BaselineInfo baselineInfo : baselineMap.values()) {
            List<Map<String, Object>> rawPoint = (List<Map<String, Object>>) JSON.parse(baselineInfo.getExtend());
            if (rawPoint != null) {
                Set<Point> points = PlanManagerUtil.jsonToPoints(baselineInfo.getParameterSql(), rawPoint);
                pointMap.put(baselineInfo.getParameterSql(), points);
            }
        }
        parametricQueryAdvisor.load(pointMap);
    }

    @Override
    protected void doDestroy() {
        try {
            if (scheduler != null) {
                scheduler.shutdown();
                scheduler.awaitTermination(20, TimeUnit.SECONDS);
            }
            if (executor != null) {
                executor.shutdown();
                executor.awaitTermination(20, TimeUnit.SECONDS);
            }
            return;
        } catch (Exception e) {
            logger.error("PlanManager scheduler/executor awaitTermination error", e);
        }
        scheduler.shutdownNow();
        executor.shutdownNow();
        baselineMap.clear();
        parametricQueryAdvisor.clear();
        sqlHistoryBloomfilter.clear();
    }

    public Map<String, BaselineInfo> getBaselineMap() {
        return baselineMap;
    }

    public ParamManager getParamManager() {
        return paramManager;
    }

    public ExecutionPlan choosePlanForPrepare(SqlParameterized sqlParameterized,
                                              SqlNodeList sqlNodeList,
                                              ExecutionContext executionContext) {
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        ExecutionPlan plan = null;
        try {
            plan = planCache.getForPrepare(sqlParameterized, plannerContext, executionContext.isTestMode());
        } catch (ExecutionException e) {
            logger.error(e);
        }
        if (plan == null) {
            // force building one plan when the preparing query has not been executed
            plan = Planner.getInstance().doBuildPlan(sqlNodeList, executionContext);
        }
        plan.setUsePostPlanner(false);
        return plan;
    }

    @Override
    public ExecutionPlan choosePlan(SqlParameterized sqlParameterized, ExecutionContext executionContext) {
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);

        // plan cache
        ExecutionPlan executionPlan = null;
        try {
            executionPlan = planCache.get(sqlParameterized, plannerContext, executionContext.isTestMode());
        } catch (ExecutionException e) {
            logger.error(e);
            return Planner.getInstance().doBuildPlan(sqlParameterized, executionContext);
        }

        if (executionPlan == null) {
            return Planner.getInstance().doBuildPlan(sqlParameterized, executionContext);
        }

        executionContext.setPlanSource(PLAN_CACHE);

        if (!(executionPlan instanceof PlaceHolderExecutionPlan)) {
            executionPlan = executionPlan.copy(executionPlan.getPlan());
        }
        executionPlan.setUsePostPlanner(
            PostPlanner.usePostPlanner(executionContext.getExplain(), executionContext.isUseHint()));
        executionPlan.setExplain(executionContext.getExplain() != null);

        if (!PlanManagerUtil.useSPM(schemaName, executionPlan, sqlParameterized.getSql(), executionContext)
            || executionContext.isTestMode()) {
            return executionPlan;
        }

        try {
            RelNode rel = doChoosePlan(executionPlan.getPlan(), executionPlan.getAst(), sqlParameterized,
                executionPlan.isExplain(), executionContext);
            return executionPlan.copy(rel);
        } catch (Throwable e) {
            logger.error("Plan Management Error", e);
            LoggerUtil.logSpmError(schemaName, "plan build error:" + sqlParameterized.getSql(), e);
            return executionPlan;
        }
    }

    private RelNode doChoosePlan(RelNode plan, SqlNode ast, SqlParameterized sqlParameterized, boolean isExplain,
                                 ExecutionContext executionContext) {
        String parameterizedSql = sqlParameterized.getSql();
        String traceId = executionContext.getTraceId();
        if (plan == null || ast == null || parameterizedSql == null || traceId == null) {
            return plan;
        }

        final int maxBaselineInfoSqlLength = paramManager.getInt(ConnectionParams.SPM_MAX_BASELINE_INFO_SQL_LENGTH);
        final int maxPlanInfoSqlLength = paramManager.getInt(ConnectionParams.SPM_MAX_PLAN_INFO_PLAN_LENGTH);
        if (parameterizedSql.length() > maxBaselineInfoSqlLength) {
            return plan;
        }

        BaselineInfo baselineInfo = baselineMap.get(parameterizedSql);
        RelOptCluster cluster = plan.getCluster();
        RelOptSchema relOptSchema =
            SqlConverter.getInstance(executionContext.getSchemaName(), executionContext).getCatalog();
        /**
         *  change plan context parameters with current sql parameters.
         *  so every plan deserialized from json could calculate cost with the right parameters.
         **/
        PlannerContext.getPlannerContext(plan).setParams(executionContext.getParams());

        Result result;
        if (baselineInfo != null) { /** select */
            result = selectPlan(baselineInfo, plan, sqlParameterized, cluster, relOptSchema, traceId,
                isExplain,
                executionContext);
        } else { /** capture */
            String planJsonString = PlanManagerUtil.relNodeToJson(plan);
            if (planJsonString.length() > maxPlanInfoSqlLength) {
                /**
                 * If the plan is too much complex, refuse capture it.
                 */
                return plan;
            }
            result = capturePlan(plan, parameterizedSql, ast, planJsonString, traceId, isExplain);
        }

        if (result.source != null) {
            executionContext.setPlanSource(result.source);
        }

        assert result.plan != null;
        if (result.plan == null) {
            logger.warn("result plan is null");
            return plan;
        }

        if (result.planInfo != null) {
            result.planInfo.addChooseCount();
            if (result.planInfo.getOrigin() == null) {
                // for drds where origin is null
                RelMetadataQuery mq = result.plan.getCluster().getMetadataQuery();
                synchronized (mq) {
                    PlannerContext.getPlannerContext(result.plan)
                        .setWorkloadType(WorkloadUtil.determineWorkloadType(result.plan, mq));
                }
            } else {
                PlannerContext.getPlannerContext(result.plan).setWorkloadType(
                    PlanManagerUtil.getWorkloadType(result.planInfo));
            }
        }
        PlannerContext.getPlannerContext(result.plan).setPlanInfo(result.planInfo);
        PlannerContext.getPlannerContext(result.plan).setExecutionContext(executionContext);
        PlannerContext.getPlannerContext(result.plan).setBaselineInfo(result.baselineInfo);
        if (!isExplain) {
            //FXIED
            recordSqlToSqlHistory(parameterizedSql);
        }
        return result.plan;
    }

    public void invalidateCache() {
        planCache.invalidate();
    }

    @Override
    public void feedBack(ExecutionPlan executionPlan, Throwable ex,
                         Map<RelNode, RuntimeStatisticsSketch> runtimeStatisticsMap,
                         ExecutionContext executionContext) {
        planCache.feedBack(executionPlan, ex);

        RelNode plan = executionPlan.getPlan();
        PlannerContext plannerContext = PlannerContext.getPlannerContext(plan);
        if (ex == null) {
            final double[] runtimeRowCount = new double[1];
            // sum the rowcount handled by this query
            runtimeStatisticsMap.entrySet().forEach(relNodeRuntimeStatisticsSketchEntry -> runtimeRowCount[0] +=
                relNodeRuntimeStatisticsSketchEntry.getValue().getRowCount());

            String parameterizedSql = executionPlan.getCacheKey().getParameterizedSql();
            final int maxBaselineSize = paramManager.getInt(ConnectionParams.SPM_MAX_BASELINE_SIZE);
            if (plannerContext.getExpectedRowcount() == -1) {
                // record first
                plannerContext.setExpectedRowcount((long) runtimeRowCount[0]);
            } else if (this.getBaselineMap().size() < maxBaselineSize &&
                !this.getBaselineMap().containsKey(parameterizedSql) &&
                !PlanManagerUtil.isTolerated(plannerContext.getExpectedRowcount(), runtimeRowCount[0],
                    executionContext.getParamManager().getInt(MINOR_TOLERANCE_VALUE))) {
                // move stmt into spm from plan cache
                BaselineInfo baselineInfo =
                    createBaselineInfo(parameterizedSql, executionPlan.getAst(), executionContext);
                this.getBaselineMap().put(parameterizedSql, baselineInfo);
                String planJsonString = PlanManagerUtil.relNodeToJson(plan);
                PlanInfo planInfo =
                    createPlanInfo(planJsonString, plan, baselineInfo.getId(), executionContext.getTraceId(),
                        PlanManagerUtil.getPlanOrigin(plan), executionPlan.getAst(), executionContext);
                baselineInfo.addAcceptedPlan(planInfo);
            }
            parametricQueryAdvisor
                .feedBack(plannerContext, plannerContext.getPlanInfo(), executionContext, runtimeStatisticsMap);
        }
    }

    public void cleanCache() {
        planCache.clean();
    }

    public ParametricQueryAdvisor getParametricQueryAdvisor() {
        return parametricQueryAdvisor;
    }

    class Result {
        public final BaselineInfo baselineInfo;
        public final PlanInfo planInfo;
        public final RelNode plan;
        public PLAN_SOURCE source;

        public Result(BaselineInfo baselineInfo, PlanInfo planInfo, RelNode plan, PLAN_SOURCE source) {
            this.baselineInfo = baselineInfo;
            this.planInfo = planInfo;
            this.plan = plan;
            this.source = source;
        }
    }

    private Result capturePlan(RelNode plan,
                               String parameterizedSql,
                               SqlNode ast,
                               String planJsonString,
                               String traceId,
                               boolean isExplain) {
        final int maxBaselineSize = paramManager.getInt(ConnectionParams.SPM_MAX_BASELINE_SIZE);
        /** only DRDS master can capture */
        if (ConfigDataMode.isMasterMode()
            && baselineMap.size() < maxBaselineSize
            && !isExplain
            && isRepeatableSql(parameterizedSql)) {

            final Set<Pair<String, String>> schemaTables = PlanManagerUtil.getTableSetFromAst(ast);
            int tablesHashCode = PlanManagerUtil.computeTablesHashCode(schemaTables, schemaName,
                PlannerContext.getPlannerContext(plan).getExecutionContext());
            if (tablesHashCode != ERROR_TABLES_HASH_CODE) {
                BaselineInfo baselineInfo = new BaselineInfo(parameterizedSql, schemaTables);
                PlanInfo resultPlanInfo = new PlanInfo(planJsonString, baselineInfo.getId(),
                    simpleCostValue(plan), traceId, PlanManagerUtil.getPlanOrigin(plan), tablesHashCode);
                baselineInfo.addAcceptedPlan(resultPlanInfo);
                /** we don't add baseline to baselineMap immediately until finishing its execution */
                return new Result(baselineInfo, resultPlanInfo, plan, SPM_NEW_BUILD);
            }
        }
        return new Result(null, null, plan, null);
    }

    private Result selectPlan(BaselineInfo baselineInfo,
                              RelNode plan,
                              SqlParameterized sqlParameterized,
                              RelOptCluster cluster,
                              RelOptSchema relOptSchema,
                              String traceId,
                              boolean isExplain, ExecutionContext executionContext) {
        Map<Integer, PlanInfo> acceptedPlans = baselineInfo.getAcceptedPlans();
        Map<Integer, PlanInfo> unacceptedPlans = baselineInfo.getUnacceptedPlans();

        assert !acceptedPlans.isEmpty(); // some concurrent case may by empty

        PlanInfo resultPlanInfo = null;
        RelNode resultPlan = null;

        /**
         * find all avalible plans
         */
        int currentHashCode = PlanManagerUtil
            .computeTablesHashCode(baselineInfo.getTableSet(), schemaName, executionContext);
        Collection<PlanInfo> avaliblePlans =
            acceptedPlans.values().stream().filter(planInfo -> planInfo.getTablesHashCode() == currentHashCode)
                .collect(Collectors.toSet());

        /**
         * Use fixed plan firstly
         */
        Optional<PlanInfo> optionalPlanInfo =
            acceptedPlans.values().stream().filter(planInfo -> planInfo.isFixed()).findFirst();
        if (optionalPlanInfo.isPresent()) {
            PLAN_SOURCE planSource = SPM_FIX;
            if (optionalPlanInfo.get().getTablesHashCode() != currentHashCode &&
                StringUtils.isNotEmpty(optionalPlanInfo.get().getFixHint())) {
                // in case of repeated updates
                synchronized (optionalPlanInfo) {
                    planSource =
                        tryUpdatePlan(optionalPlanInfo.get(), sqlParameterized, cluster, relOptSchema, currentHashCode,
                            executionContext);
                }
            }
            resultPlan = optionalPlanInfo.get().getPlan(cluster, relOptSchema);
            return new Result(baselineInfo, optionalPlanInfo.get(), resultPlan, planSource);
        }

        /**
         * weed out plan
         */
        final int maxAcceptedPlanSizePerBaseline =
            paramManager.getInt(ConnectionParams.SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE);
        if (acceptedPlans.size() >= maxAcceptedPlanSizePerBaseline) {
            PlanInfo toRemoveAccPlan = null;
            for (PlanInfo accPlan : acceptedPlans.values()) {
                if (toRemoveAccPlan == null || accPlan.getChooseCount() < toRemoveAccPlan.getChooseCount()) {
                    if (!accPlan.isFixed()) {
                        toRemoveAccPlan = accPlan;
                    }
                }
            }
            baselineInfo.removeAcceptedPlan(toRemoveAccPlan.getId());
        }

        /**
         *  use CBO to find best plan from baseline accepted plans
         */
        Point point = null;
        PlannerContext plannerContext = PlannerContext.getPlannerContext(plan);
        if (plannerContext.getExprMap() == null) {
            plannerContext.setExprMap(getRexNodeTableMap(plan));
        }
        final int maxPqoParamsSize = paramManager.getInt(ConnectionParams.SPM_MAX_PQO_PARAMS_SIZE);
        if (plannerContext.getExprMap() != null && // 参数推导模块已推导出有效的表达式-> tbl 映射
            plannerContext.getExprMap().size() > 0 &&
            avaliblePlans != null && // 可用 plan 不为空
            avaliblePlans.size() > 0 && // 可用 plan 数量超过 1
            sqlParameterized.getParameters().size() < maxPqoParamsSize // stmt 参数的数量小于 pqo 模块设定参数的最大值
        ) {
            // 进入 PQO
            Pair<Point, PlanInfo> planInfoPair = parametricQueryAdvisor
                .advise(sqlParameterized.getSql(), avaliblePlans,
                    sqlParameterized.getParameters(),
                    plannerContext, executionContext, isExplain);
            point = planInfoPair.getKey();
            executionContext.setPoint(point);
            if (planInfoPair.getValue() != null) {
                return new Result(baselineInfo, planInfoPair.getValue(),
                    planInfoPair.getValue().getPlan(cluster, relOptSchema), SPM_PQO);
            } else {
                if (isExplain) {
                    return new Result(baselineInfo, null, plan, PLAN_CACHE);
                }
                // 为该参数空间生成一个新的 plan
                Result result =
                    buildNewPlan(cluster, relOptSchema, baselineInfo, sqlParameterized, executionContext,
                        currentHashCode);
                point.setPlanId(result.planInfo.getId());
                return result;
            }
        } else {
            // try unaccepted plan
            if (unacceptedPlans.size() > 0 &&
                ConfigDataMode.isMasterMode() &&
                !isExplain &&
                rand.nextDouble() < paramManager.getFloat(ConnectionParams.SPM_EVOLUTION_RATE)) {

                /** should give a chance for unaccepted plan */
                List<Integer> toRemoveList = new ArrayList<>();
                PlanInfo unacceptedPlan =
                    findMinCostPlan(cluster, relOptSchema, unacceptedPlans.values(), baselineInfo, executionContext,
                        toRemoveList);
                if (unacceptedPlan != null) {
                    return new Result(baselineInfo, unacceptedPlan, unacceptedPlan.getPlan(cluster, relOptSchema),
                            PLAN_SOURCE.SPM_UNACCEPTED);
                }
            }

            // record params for evolution
            Map<Integer, ParameterContext> parameterContextMap = executionContext.getParams().getCurrentParameter();
            if (parametersCache.get(baselineInfo) == null && parameterContextMap != null
                && parameterContextMap.size() < 10) {
                parametersCache.put(baselineInfo, parameterContextMap);
            }

            // find best plan from baseline accepted plans
            logger.warn("find null point, sql:" + sqlParameterized + ", exprMap:" + plannerContext.getExprMap()
                + ", avaliblePlanSize: " + avaliblePlans.size());
            resultPlanInfo =
                findMinCostPlan(cluster, relOptSchema, avaliblePlans, baselineInfo, executionContext, null);
            if (resultPlanInfo != null) {
                // min cost plan
                return new Result(baselineInfo, resultPlanInfo, resultPlanInfo.getPlan(cluster, relOptSchema),
                    PLAN_SOURCE.SPM_ACCEPT);
            } else {
                if (isExplain) {
                    return new Result(baselineInfo, null, plan, PLAN_CACHE);
                }
                // new plan
                Result result = buildNewPlan(cluster, relOptSchema, baselineInfo, sqlParameterized, executionContext,
                    currentHashCode);
                return result;
            }
        }
    }

    private Result buildNewPlan(RelOptCluster cluster, RelOptSchema relOptSchema, BaselineInfo baselineInfo,
                                SqlParameterized sqlParameterized, ExecutionContext executionContext,
                                int currentHashCode) {
        logger.info("build new plan, sql:" + baselineInfo.getParameterSql());
        ExecutionPlan planWithContext = Planner.getInstance().doBuildPlan(sqlParameterized, executionContext);
        RelNode p = planWithContext.getPlan();
        PlanInfo resultPlanInfo =
            new PlanInfo(PlanManagerUtil.relNodeToJson(p), baselineInfo.getId(), simpleCostValue(p),
                executionContext.getTraceId()
                , PlanManagerUtil.getPlanOrigin(p), currentHashCode);
        resultPlanInfo.setAccepted(true);
        baselineInfo.getAcceptedPlans().put(resultPlanInfo.getId(), resultPlanInfo);
        return new Result(baselineInfo, resultPlanInfo, resultPlanInfo.getPlan(cluster, relOptSchema),
            SPM_NEW_BUILD);
    }

    private PLAN_SOURCE tryUpdatePlan(PlanInfo planInfo,
                                      SqlParameterized sqlParameterized,
                                      RelOptCluster cluster,
                                      RelOptSchema relOptSchema, int currentHashCode,
                                      ExecutionContext executionContext) {
        RelNode oldPlan = planInfo.getPlan(cluster, relOptSchema);
        ExecutionPlan newPlan =
            Planner.getInstance()
                .plan(planInfo.getFixHint() + " " + sqlParameterized.getSql(), executionContext.copy());

        PlanManagerUtil.jsonToRelNode(planInfo.getPlanJsonString(), cluster, relOptSchema);
        // if row type changed(select * xxx), meaning plan should be rebuild
        RelDataType oldRowType = oldPlan.getRowType();
        RelDataType newRowType = newPlan.getPlan().getRowType();

        // plan needed to be rebuild in two cases:
        // 1: row type changed
        // 2: old plan is not valid anymore
        if (!oldRowType.getFullTypeString().equalsIgnoreCase(newRowType.getFullTypeString())) {
            planInfo.resetPlan(newPlan.getPlan());
            planInfo.setTablesHashCode(currentHashCode);
            loggerSpm.warn(
                "fix plan being rebuilt for row type change, fix hint:" + planInfo.getFixHint()
                    + ", stmt:" + sqlParameterized.getSql()
                    + ", old row type:" + oldRowType.getFullTypeString()
                    + ", new row type:" + newRowType.getFullTypeString()
            );
            return PLAN_SOURCE.SPM_FIX_PLAN_UPDATE_FOR_ROW_TYPE;
        } else if (!oldPlan.isValid(IGNORE, null)) {
            planInfo.resetPlan(newPlan.getPlan());
            planInfo.setTablesHashCode(currentHashCode);
            loggerSpm.warn(
                "fix plan being rebuilt for plan valid check, fix hint:" + planInfo.getFixHint()
                    + ", stmt:" + sqlParameterized.getSql()
                    + ", old plan:" + oldPlan.getDigest()
            );
            return PLAN_SOURCE.SPM_FIX_PLAN_UPDATE_FOR_INVALID;
        } else {
            planInfo.setTablesHashCode(currentHashCode);
            loggerSpm.warn("fix plan reset ddl hashcode, fix hint:" + planInfo.getFixHint()
                + ", stmt:" + sqlParameterized.getSql()
                + ", tables:" + newPlan.getTableSet()
            );
            return PLAN_SOURCE.SPM_FIX_DDL_HASHCODE_UPDATE;
        }
    }

    /**
     * get the columns involved in the workload(recently)
     * plus all columns in the index
     */
    public Map<String, Set<String>> columnsInvolvedByPlan() {
        Map<String, Set<String>> columnsMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

        // handle plan cache
        for (ExecutionPlan executionPlan : planCache.getCache().asMap().values()) {
            // table name -> column name collection
            Map<String, Set<String>> columnsMapTmp = getColumnsFromPlan(schemaName, executionPlan.getPlan());
            mergeColumns(columnsMap, columnsMapTmp);
        }

        // handle baseline
        for (BaselineInfo baselineInfo : baselineMap.values()) {
            for (PlanInfo planInfo : baselineInfo.getPlans()) {
                RelNode plan = planInfo.getPlan(null, null);
                if (plan != null && isRecentlyExecuted(planInfo)) {
                    // table name -> column name collection
                    Map<String, Set<String>> columnsMapTmp = getColumnsFromPlan(schemaName, plan);
                    mergeColumns(columnsMap, columnsMapTmp);
                }
            }
        }
        return columnsMap;
    }

    /**
     * judge if a plan executed recently.(default one week)
     */
    private boolean isRecentlyExecuted(PlanInfo planInfo) {
        if (planInfo.getLastExecuteTime() == null) {
            return false;
        }
        long recentTimePeriod =
            OptimizerContext.getContext(schemaName).getParamManager().getLong(SPM_RECENTLY_EXECUTED_PERIOD);

        if (System.currentTimeMillis() - planInfo.getLastExecuteTime() > recentTimePeriod) {
            return false;
        }
        return true;
    }

    public PlanInfo findMinCostPlan(RelOptCluster cluster, RelOptSchema relOptSchema, Collection<PlanInfo> planInfos,
                                    BaselineInfo baselineInfo,
                                    ExecutionContext executionContext, Collection<Integer> toBeRemovedPlan) {
        if (planInfos.size() == 0) {
            return null;
        } else if (planInfos.size() == 1) {
            return planInfos.iterator().next();
        }
        int tablesHashCode = PlanManagerUtil
            .computeTablesHashCode(baselineInfo.getTableSet(), schemaName, executionContext);
        RelOptCost bestCost = null;
        PlanInfo basePlan = null;
        for (PlanInfo planInfo : planInfos) {
            if (planInfo.getTablesHashCode() != tablesHashCode) {
                continue;
            }
            try {
                planInfo.getPlan(cluster, relOptSchema);
            } catch (Throwable e) {
                logger.error("Plan Management Error", e);
                LoggerUtil.logSpmError(schemaName, "plan build error:" + planInfo.getPlanJsonString(), e);
                if (toBeRemovedPlan != null) {
                    toBeRemovedPlan.add(planInfo.getId());
                }
                continue;
            }

            if (bestCost == null) { // delay calculating acceptedPlanInfo cost
                bestCost = planInfo.getCumulativeCost(cluster, relOptSchema, executionContext.getParams());
                basePlan = planInfo;
                continue;
            }
            RelOptCost cost = planInfo.getCumulativeCost(cluster, relOptSchema, executionContext.getParams());
            if (cost.isLt(bestCost)) {
                bestCost = cost;
                basePlan = planInfo;
            }
        }
        return basePlan;
    }

    @Override
    public void doEvolution(BaselineInfo baselineInfo, PlanInfo planInfo, long lastExecuteUnixTime,
                            double executionTimeInSeconds, Throwable ex) {
        if (ex != null) {
            // something error, maybe: user kill the sql or plan externalization is not compatible or bug
            int errorCount = planInfo.incrementAndGetErrorCount();
            logger.error("plan Management error : planInfo execute error, " +
                "BaselineInfoId = " + baselineInfo.getId() + ", planInfoId = " + planInfo.getId() +
                ",errorCount = " + errorCount + ",executionTimeInSeconds = " + executionTimeInSeconds, ex);
            final int maxPlanInfoErrorCount =
                this.getParamManager().getInt(ConnectionParams.SPM_MAX_PLAN_INFO_ERROR_COUNT);
            if (errorCount >= maxPlanInfoErrorCount) {
                // planInfo errorCount larger than SPM_MAX_PLAN_INFO_ERROR_COUNT
                // if planInfo is the only acceptedPlan. notifyDeleteBaseline
                baselineInfo.removeUnacceptedPlan(planInfo.getId());
                if (baselineInfo.getAcceptedPlans().containsKey(planInfo.getId())
                    && baselineInfo.getAcceptedPlans().size() == 1) {
                    baselineMap.remove(baselineInfo.getParameterSql());
                    notifyDeleteBaselineAsync(baselineInfo);
                } else {
                    baselineInfo.removeAcceptedPlan(planInfo.getId());
                }
            }
            return;
        }
        // plan maybe become normal
        planInfo.zeroErrorCount();
        boolean needPersist = false;
        if (baselineMap.get(baselineInfo.getParameterSql()) == null) {
            /** capture plan should add to baselineMap when execute successfully */
            baselineMap.put(baselineInfo.getParameterSql(), baselineInfo);
            needPersist = true;
        }
        planInfo.addChooseCount();
        planInfo.setLastExecuteTime(lastExecuteUnixTime);
        planInfo.updateEstimateExecutionTime(executionTimeInSeconds);
        Map<Integer, PlanInfo> acceptedPlans = baselineInfo.getAcceptedPlans();
        Map<Integer, PlanInfo> unacceptedPlans = baselineInfo.getUnacceptedPlans();
        final int maxAcceptedPlanSizePerBaseline =
            paramManager.getInt(ConnectionParams.SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE);
        if (unacceptedPlans.containsKey(planInfo.getId())) {
            for (PlanInfo acceptedPlanInfo : acceptedPlans.values()) {
                if (planInfo.getEstimateExecutionTime() < acceptedPlanInfo.getEstimateExecutionTime()) {
                    /** evolution succeed */
                    baselineInfo.removeUnacceptedPlan(planInfo.getId());
                    if (acceptedPlans.size() >= maxAcceptedPlanSizePerBaseline) {
                        baselineInfo.removeAcceptedPlan(acceptedPlanInfo.getId());
                    }
                    baselineInfo.addAcceptedPlan(planInfo);
                    needPersist = true;
                    LoggerUtil.logSpm(schemaName, "plan evolution:" + planInfo.getId());
                    break;
                }
            }
        }

        if (needPersist) {
            // Async persist when plan enter accepted list and sync
            if (TddlNode.isCurrentNodeMaster()) {
                persistAsync(baselineInfo);
            }
            notifyUpdateBaselineAsync(baselineInfo);
        } else if (planInfo.isAccepted()) {
            baselineInfo.setDirty(true);
        }
    }

    private boolean isRepeatableSql(String parameterizedSql) {
        return sqlHistoryBloomfilter.mightContainSynchronized(parameterizedSql);
    }

    private void recordSqlToSqlHistory(String parameterizedSql) {
        sqlHistoryBloomfilter.putSynchronized(parameterizedSql);
    }

    /**
     * only callback by baselineSyncController
     */
    @Override
    public void updateBaseline(BaselineInfo otherBaselineInfo) {
        final int maxBaselineSize = paramManager.getInt(ConnectionParams.SPM_MAX_BASELINE_SIZE);
        final int maxAcceptedPlanSizePerBaseline =
            paramManager.getInt(ConnectionParams.SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE);
        BaselineInfo baselineInfo = baselineMap.get(otherBaselineInfo.getParameterSql());
        boolean needPersist = false;
        if (baselineInfo != null) {
            for (PlanInfo planInfo : otherBaselineInfo.getAcceptedPlans().values()) {
                if (baselineInfo.getAcceptedPlans().size() < maxAcceptedPlanSizePerBaseline) {
                    int beforeSize = baselineInfo.getAcceptedPlans().size();
                    baselineInfo.addAcceptedPlan(planInfo);
                    int afterSize = baselineInfo.getAcceptedPlans().size();
                    if (afterSize > beforeSize) {
                        needPersist = true;
                    }
                }
            }
        } else {
            if (baselineMap.size() < maxBaselineSize) {
                baselineMap.put(otherBaselineInfo.getParameterSql(), otherBaselineInfo);
                needPersist = true;
            }
        }
        // Async persist when plan enter accepted list
        if (needPersist && TddlNode.isCurrentNodeMaster()) {
            persistAsync(baselineInfo);
        }
    }

    /**
     * only callback by baselineSyncController
     */
    @Override
    public void deleteBaseline(int baselineInfoId, String parameterSql) {
        baselineMap.remove(parameterSql);
        parametricQueryAdvisor.remove(parameterSql);
        if (TddlNode.isCurrentNodeMaster()) {
            deleteFromDatabaseAsync(baselineInfoId);
        }
    }

    /**
     * only callback by baselineSyncController
     */
    @Override
    public void deleteBaseline(int baselineInfoId, String parameterSql, int planInfoId) {
        BaselineInfo baselineInfo = baselineMap.get(parameterSql);
        baselineInfo.removeAcceptedPlan(planInfoId);
        baselineInfo.removeUnacceptedPlan(planInfoId);
        if (TddlNode.isCurrentNodeMaster()) {
            if (baselineInfo.getAcceptedPlans().isEmpty()) {
                baselineMap.remove(parameterSql);
                deleteFromDatabaseAsync(baselineInfoId);
            } else {
                deleteFromDatabaseAsync(baselineInfoId, planInfoId);
            }
        }
    }

    /**
     * only callback by baselineSyncController
     */
    @Override
    public void updatePlanInfo(PlanInfo planInfo, String parameterSql, int originPlanId) {
        BaselineInfo baselineInfo = baselineMap.get(parameterSql);
        if (planInfo.isAccepted()) {
            baselineInfo.removeAcceptedPlan(originPlanId);
            baselineInfo.getAcceptedPlans().put(planInfo.getId(), planInfo);
        } else {
            baselineInfo.removeUnacceptedPlan(originPlanId);
            baselineInfo.getUnacceptedPlans().put(planInfo.getId(), planInfo);
        }
        if (TddlNode.isCurrentNodeMaster()) {
            updateFromDatabaseAsync(baselineInfo, planInfo, originPlanId);
        }
    }

    private void notifyUpdateBaselineAsync(BaselineInfo baselineInfo) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
                    baselineSyncController.updateBaseline(schemaName, baselineInfo);
                }
            }
        });
    }

    public void notifyUpdatePlanAsync(
        ExecutionPlan originPlan, BaselineInfo baselineInfo, PlanInfo originPlanInfo,
        WorkloadType feedBackWorkload,
        ExecutionContext newExecutionContext) {
        notifyUpdatePlanAsync(
            originPlan, baselineInfo, originPlanInfo, feedBackWorkload, newExecutionContext, executor);
    }

    public void notifyUpdatePlanAsync(
        ExecutionPlan originPlan, BaselineInfo baselineInfo, PlanInfo originPlanInfo,
        WorkloadType feedBackWorkload,
        ExecutionContext newExecutionContext, Executor threadPoolExecutor) {
        if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
            try {
                newExecutionContext.setWorkloadType(null);
                newExecutionContext.setEnableFeedBackWorkload(true);

                Map<String, Object> extraCmd = new HashMap<>();
                extraCmd.putAll(newExecutionContext.getExtraCmds());
                extraCmd.put(ConnectionProperties.WORKLOAD_TYPE, feedBackWorkload.name());
                newExecutionContext.setExtraCmds(extraCmd);
                ExecutionPlan targetExecutionPlan =
                    Planner.getInstance().plan(newExecutionContext.getOriginSql(), newExecutionContext);
                threadPoolExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if (targetExecutionPlan != null) {
                                RelNode targetPlan = targetExecutionPlan.getPlan();
                                PlannerContext targetContext = PlannerContext.getPlannerContext(targetPlan);
                                if (feedBackWorkload == targetContext.getWorkloadType()) {
                                    if (originPlan.getCacheKey() != null) {
                                        targetExecutionPlan.saveCacheState(
                                            originPlan.getTableSet(), originPlan.getTableSetHashCode(),
                                            originPlan.getCacheKey(), originPlan.getTableMetaSnapshots());
                                        targetExecutionPlan
                                            .setPrivilegeVerifyItems(originPlan.getPrivilegeVerifyItems());
                                        planCache.putCachePlan(originPlan.getCacheKey(), targetExecutionPlan);
                                    }
                                    if (baselineInfo != null && originPlanInfo != null) {
                                        SqlNode ast = targetExecutionPlan.getAst();
                                        String planJsonString = PlanManagerUtil.relNodeToJson(targetPlan);
                                        PlanInfo planInfo =
                                            createPlanInfo(
                                                planJsonString, targetPlan, baselineInfo.getId(),
                                                newExecutionContext.getTraceId(),
                                                PlanManagerUtil.getPlanOrigin(targetPlan),
                                                ast, newExecutionContext);
                                        planInfo.setAccepted(originPlanInfo.isAccepted());
                                        baselineSyncController.updatePlanInfo(
                                            schemaName, originPlanInfo.getId(), baselineInfo, planInfo);
                                    }

                                    logger.info("Feedback the workload for " + newExecutionContext.getTraceId());
                                }
                            }
                        } catch (Throwable t) {
                            logger.warn("notifyUpdatePlanAsync failed!", t);
                        }
                    }
                });
            } catch (Throwable t) {
                logger.warn("notifyUpdatePlanAsync failed!", t);
            }
        }
    }

    private void notifyDeleteBaselineAsync(BaselineInfo baselineInfo) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
                    baselineSyncController.deleteBaseline(schemaName, baselineInfo);
                }
            }
        });
    }

    private void deleteFromDatabaseAsync(int baselineInfoId) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
                    systemTableBaselineInfo.delete(baselineInfoId);
                }
            }
        });
    }

    private void deleteFromDatabaseAsync(int baselineInfoId, int planInfoId) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
                    systemTableBaselineInfo.deletePlan(baselineInfoId, planInfoId);
                }
            }
        });
    }

    private void updateFromDatabaseAsync(BaselineInfo baselineInfo, PlanInfo updatePlanInfo, int originPlanId) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
                    systemTableBaselineInfo.updatePlan(baselineInfo, updatePlanInfo, originPlanId);
                }
            }
        });
    }

    private void persistAsync(BaselineInfo baselineInfo) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
                    SystemTableBaselineInfo.PersistResult persistResult =
                        systemTableBaselineInfo.persist(baselineInfo);
                    if (persistResult == SystemTableBaselineInfo.PersistResult.CONFLICT) {
                        // TODO should sync delete BaselineInfo ?
                        logger.error("conflict appear with baselineId = " + baselineInfo.getId());
                    }
                }
            }
        });
    }

    private void loadBaseLineInfoAndPlanInfo() {
        systemTableBaselineInfo.loadData(this, 0);
        lastReadTime = unixTimeStamp();
    }

    @Override
    public boolean checkBaselineHashCodeValid(BaselineInfo baselineInfo, PlanInfo planInfo) {
        return false;
    }

    private void startReadForeverAsync() {
        PlanManager planManager = this;
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM) && paramManager
                    .getBoolean(ConnectionParams.ENABLE_SPM_BACKGROUND_TASK)) {
                    logger.debug("plan manager async load data with lastReadTime = " + lastReadTime);
                    systemTableBaselineInfo.loadData(planManager, lastReadTime);
                    lastReadTime = unixTimeStamp();
                }
            }
        }, 300, 300, TimeUnit.SECONDS);
    }

    private void startCheckForeverAsync() {
        PlanManager planManager = this;
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM) && paramManager
                    .getBoolean(ConnectionParams.ENABLE_SPM_BACKGROUND_TASK)) {
                    logger.debug("plan manager async check data");
                    planManager.forceValidateAll();
                }
            }
        }, 300, 300, TimeUnit.SECONDS);
    }

    private void startFlushForeverAsync() {
        PlanManager planManager = this;
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM) && paramManager
                    .getBoolean(ConnectionParams.ENABLE_SPM_BACKGROUND_TASK)) {
                    logger.debug("plan manager async flush data ");
                    for (BaselineInfo baselineInfo : planManager.getBaselineMap().values()) {
                        if (baselineInfo.isDirty()) {
                            systemTableBaselineInfo.persist(baselineInfo);
                            baselineInfo.setDirty(false);
                        }
                    }
                }
            }
        }, 1, 1, TimeUnit.HOURS);
    }

    private void startCheckPlan() {
        PlanManager planManager = this;
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM) && paramManager
                    .getBoolean(ConnectionParams.ENABLE_SPM_BACKGROUND_TASK)) {
                    logger.debug("plan manager async flush data ");
                    parametricQueryAdvisor.checkPlanRedundant(paramManager.getInt(MINOR_TOLERANCE_VALUE));
                    handlePlanEvolution();
                }
            }
        }, 1, 5, TimeUnit.SECONDS);
    }

    private double simpleCostValue(RelNode plan) {
        /** simple cost, we do not use synchronized and parameters to get precise value */
        RelMetadataQuery mq = plan.getCluster().getMetadataQuery();
        synchronized (mq) {
            RelOptCost cost = mq.getCumulativeCost(plan);
            return cost.getCpu()
                + CostModelWeight.INSTANCE.getIoWeight() * cost.getIo()
                + CostModelWeight.INSTANCE.getNetWeight() * cost.getNet();
        }
    }

    private void handlePlanEvolution() {
        final int maxAcceptedPlanSizePerBaseline =
            paramManager.getInt(ConnectionParams.SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE);
        final int maxUnacceptedPlanSizePerBaseline =
            paramManager.getInt(ConnectionParams.SPM_MAX_UNACCEPTED_PLAN_SIZE_PER_BASELINE);
        for (BaselineInfo baselineInfo : this.getBaselineMap().values()) {
            /**
             * weed out plan
             */
            Map<Integer, PlanInfo> acceptedPlans = baselineInfo.getAcceptedPlans();
            if (acceptedPlans.size() > maxAcceptedPlanSizePerBaseline) {
                PlanInfo toRemoveAccPlan = null;
                for (PlanInfo accPlan : acceptedPlans.values()) {
                    if (toRemoveAccPlan == null || accPlan.getChooseCount() < toRemoveAccPlan
                        .getChooseCount()) {
                        if (!accPlan.isFixed()) {
                            toRemoveAccPlan = accPlan;
                        }
                    }
                }
                baselineInfo.removeAcceptedPlan(toRemoveAccPlan.getId());
            }

            Map<Integer, PlanInfo> unacceptedPlans = baselineInfo.getUnacceptedPlans();
            if (unacceptedPlans.size() > maxUnacceptedPlanSizePerBaseline) {
                PlanInfo toRemoveUnaccPlan = null;
                for (PlanInfo unaccPlan : unacceptedPlans.values()) {
                    if (toRemoveUnaccPlan == null || unaccPlan.getChooseCount() < toRemoveUnaccPlan
                        .getChooseCount()) {
                        toRemoveUnaccPlan = unaccPlan;
                    }
                }
                baselineInfo.removeUnacceptedPlan(toRemoveUnaccPlan.getId());
            }

            PlanInfo planInfo = null;
            RelNode plan = null;
            Iterator<PlanInfo> iterator = baselineInfo.getAcceptedPlans().values().iterator();
            while (iterator.hasNext()) {
                planInfo = iterator.next();
                plan = planInfo.getPlan(null, null);
                if (plan != null) {
                    break;
                }
            }

            if (plan == null) {
                // 没有找到合适的 plan
                continue;
            }

            // 如果 accept plan 和 unaccept plan 的数量某个超限的话, 停止演化
            if (acceptedPlans.size() >= maxAcceptedPlanSizePerBaseline ||
                unacceptedPlans.size() >= maxUnacceptedPlanSizePerBaseline) {
                continue;
            }

            // 异步构造 plan 并将之放入 unaccept plan 中
            Map<Integer, ParameterContext> parameters = parametersCache.get(baselineInfo);
            ExecutionContext executionContext =
                PlannerContext.getPlannerContext(plan).getExecutionContext();
            SqlParameterized sqlParameterized =
                SqlParameterizeUtils.parameterize(baselineInfo.getParameterSql(), parameters);
            ExecutionPlan planWithContext =
                Planner.getInstance().doBuildPlan(sqlParameterized, executionContext);
            String planJsonString = PlanManagerUtil.relNodeToJson(planWithContext.getPlan());
            if (!baselineInfo.getAcceptedPlans().containsKey(planJsonString.hashCode()) &&
                !baselineInfo.getUnacceptedPlans().containsKey(planJsonString.hashCode())) {
                int currentHashCode = PlanManagerUtil
                    .computeTablesHashCode(baselineInfo.getTableSet(), schemaName, executionContext);
                PlanInfo evolutionPlanInfo =
                    new PlanInfo(planJsonString, baselineInfo.getId(), simpleCostValue(plan), "0000"
                        , PlanManagerUtil.getPlanOrigin(plan), currentHashCode);
                baselineInfo.getUnacceptedPlans().put(planJsonString.hashCode(), evolutionPlanInfo);
            }

        }
        parametersCache.clear();
    }

    @Override
    public void forcePersist(Integer baselineId) {
        if (baselineId != null) {
            innerForcePersist(baselineId);
        }
    }

    @Override
    public void forcePersistAll() {
        innerForcePersist(null);
    }

    private void innerForcePersist(Integer baselineId) {
        if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM) && TddlNode.isCurrentNodeMaster()) {
            for (BaselineInfo baselineInfo : baselineMap.values()) {
                if (baselineId != null && baselineId != baselineInfo.getId()) {
                    continue;
                }
                baselineInfo.setExtend(
                    PlanManagerUtil.pointsToJson(parametricQueryAdvisor.dump().get(baselineInfo.getParameterSql())));
                SystemTableBaselineInfo.PersistResult persistResult = systemTableBaselineInfo.persist(baselineInfo);
                if (persistResult == SystemTableBaselineInfo.PersistResult.CONFLICT) {
                    // TODO should sync delete BaselineInfo ?
                    logger.error("conflict appear with baselineId = " + baselineInfo.getId());
                }
            }
        }
    }

    @Override
    public void forceClearAll() {
        if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
            // only delete memory data
            baselineMap.clear();
            parametricQueryAdvisor.clear();
        }
    }

    @Override
    public void forceClear(Integer baselineId) {
        if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
            for (BaselineInfo baselineInfo : baselineMap.values()) {
                if (baselineInfo.getId() == baselineId) {
                    baselineMap.remove(baselineInfo.getParameterSql());
                    parametricQueryAdvisor.remove(baselineInfo.getParameterSql());
                    return;
                }
            }
        }
    }

    @Override
    public void forceLoad(Integer baselineId) {
        if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
            if (baselineId != null) {
                systemTableBaselineInfo.loadData(this, 0, baselineId);
            }
        }
    }

    @Override
    public void forceLoadAll() {
        if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
            systemTableBaselineInfo.loadData(this, 0);
        }
    }

    @Override
    public void forceValidateAll() {
        innerForceValidate(null);
    }

    @Override
    public void forceValidate(Integer baselineId) {
        if (baselineId != null) {
            innerForceValidate(baselineId);
        }
    }

    private void innerForceValidate(Integer baselineId) {
        if (paramManager.getBoolean(ConnectionParams.ENABLE_SPM)) {
            for (BaselineInfo baselineInfo : baselineMap.values()) {
                if (baselineId != null && baselineId != baselineInfo.getId()) {
                    continue;
                }
                baselineInfo.removeInvalidUnacceptedPlans();
            }

        }
    }

    @Override
    public BaselineInfo createBaselineInfo(String parameterizedSql, SqlNode ast, ExecutionContext ec) {
        final Set<Pair<String, String>> schemaTables = PlanManagerUtil.getTableSetFromAst(ast);
        return new BaselineInfo(parameterizedSql, schemaTables);
    }

    @Override
    public PlanInfo createPlanInfo(String planJsonString, RelNode plan, int baselinInfoId, String traceId,
                                   String origin, SqlNode ast,
                                   ExecutionContext executionContext) {
        final Set<Pair<String, String>> schemaTables = PlanManagerUtil.getTableSetFromAst(ast);
        int tablesHashCode = PlanManagerUtil.computeTablesHashCode(schemaTables, schemaName, executionContext);
        return new PlanInfo(planJsonString, baselinInfoId, simpleCostValue(plan), traceId, origin, tablesHashCode);
    }

    public PlanCache getPlanCache() {
        return planCache;
    }

    public enum PLAN_SOURCE {
        PLAN_CACHE, SPM_FIX, SPM_PQO, SPM_UNACCEPTED, SPM_ACCEPT, SPM_NEW_BUILD, SPM_FIX_PLAN_UPDATE_FOR_ROW_TYPE,
        SPM_FIX_PLAN_UPDATE_FOR_INVALID, SPM_FIX_DDL_HASHCODE_UPDATE
    }
}

