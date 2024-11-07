package com.alibaba.polardbx.optimizer.planmanager.hotevolution;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.stats.metric.FeatureStats;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang.StringUtils;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.stats.metric.FeatureStatsItem.HOT_EVOLVE_PLAN_NUM;

public class HotGsiEvolution implements Runnable {
    // schedule thread to evolve plan every 1s
    private final ScheduledThreadPoolExecutor hotEvoluteThread =
        ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("Hot-Gsi-Evolve-Thread", true),
            new ThreadPoolExecutor.DiscardPolicy());

    private final static int QUEUE_CAPACITY = 100;
    private final static long INIT_DELAY = 5L;
    private final static HotGsiEvolution INSTANCE = new HotGsiEvolution(QUEUE_CAPACITY, INIT_DELAY);

    public static HotGsiEvolution getInstance() {
        return INSTANCE;
    }

    private final BlockingQueue<GsiEvolutionInfo> evolutionQueue;

    public HotGsiEvolution(int capacity, long delay) {
        this.evolutionQueue = new LinkedBlockingQueue<>(capacity);
        this.hotEvoluteThread.scheduleWithFixedDelay(
            this,
            delay,
            1,
            TimeUnit.SECONDS
        );
    }

    public HotGsiEvolution(int size) {
        this.evolutionQueue = new LinkedBlockingQueue<>(size);
    }

    public boolean submitEvolutionTask(GsiEvolutionInfo info) {
        return evolutionQueue.offer(info);
    }

    public int getQueueSize() {
        return evolutionQueue.size();
    }

    @Override
    public void run() {
        GsiEvolutionInfo info;
        while ((info = evolutionQueue.poll()) != null) {
            evolution(info);
        }
    }

    /**
     * entry point to evolve plan
     *
     * @param info info of plan to evolved
     */
    public static void evolution(GsiEvolutionInfo info) {
        try {
            SqlParameterized sqlParameterized = info.getSqlParameterized();
            if (sqlParameterized == null) {
                return;
            }
            // check ENABLE_HOT_GSI_EVOLUTION again in case it is disabled between capture and evolution
            if (!DynamicConfig.getInstance().enableHotGsiEvolution()) {
                return;
            }

            ExecutionContext executionContext = info.getExecutionContext();
            ExecutionPlan executionPlan = info.getExecutionPlan();
            PlanManager planManager = PlanManager.getInstance();
            String schema = executionContext.getSchemaName();
            schema = schema.toLowerCase(Locale.ROOT);
            String parameterizedSql = sqlParameterized.getSql();

            // plan cache is full
            if (PlanCache.getInstance().getCache().size() >= PlanCache.getInstance().getCurrentCapacity() - 20) {
                LoggerUtil.logSpm(schema,
                    String.format("HGE: schema [%s] tid [%s] fails to evolute due to plan cache full", schema,
                        info.getTemplateId()));
                return;
            }

            // get the baseline info of current sql
            BaselineInfo baselineInfo = planManager.getBaselineMap(schema).get(parameterizedSql);
            if (baselineInfo == null) {
                baselineInfo = planManager.addBaselineInfo(schema, parameterizedSql,
                    planManager.createBaselineInfo(parameterizedSql, executionPlan.getAst(), executionContext));
            }
            // baseline is full
            if (baselineInfo == null) {
                LoggerUtil.logSpm(schema,
                    String.format("HGE: schema [%s] tid [%s] fails to evolute due to spm full", schema,
                        info.getTemplateId()));
                return;
            }
            // don't evolve if the sql is evolved already
            if (!baselineInfo.canHotEvolution()) {
                LoggerUtil.logSpm(schema,
                    String.format("HGE: schema [%s] tid [%s] bid [%s] fails to evolute due to baseline evolved",
                        schema, info.getTemplateId(), baselineInfo.getId()));
                return;
            }
            // add origin plan and plan without gsi
            if (!addPlanDefaultAndWithoutGsi(sqlParameterized, executionContext, executionPlan, baselineInfo, info)) {
                return;
            }
            Set<String> ignoredGsi = Sets.newTreeSet(String::compareToIgnoreCase);
            ignoredGsi.add(info.getIndexName());
            do {
                executionContext = executionContext.copyContextForOptimizer();
                executionContext.setIgnoredGsi(ignoredGsi);
                executionPlan = Planner.getInstance().doBuildPlan(sqlParameterized, executionContext);
                String indexName = HotGsiCapture.candidateHotGsi(executionContext, executionPlan.getPlan());
                // can't find hot gsi
                if (StringUtils.isEmpty(indexName)) {
                    return;
                }
                ignoredGsi.add(indexName);
            } while (addNewPlanInfo(executionContext, executionPlan, baselineInfo, info));
        } catch (Throwable t) {
            LoggerUtil.logSpm(info.getExecutionContext().getSchemaName(), t.getMessage());
        }
    }

    /**
     * gen plan without index selection, mark the baseline as HotEvolution and add it to baseline
     *
     * @param sqlParameterized origin sql
     * @param executionContext original execution context
     * @param executionPlan original execution plan
     * @param baselineInfo baseline info of the sql
     * @return true if plan without gsi is added
     */
    private static boolean addPlanDefaultAndWithoutGsi(
        SqlParameterized sqlParameterized,
        ExecutionContext executionContext,
        ExecutionPlan executionPlan,
        BaselineInfo baselineInfo,
        GsiEvolutionInfo info) {
        // build plan without index_selection
        ExecutionContext newExecutionContext = executionContext.copyContextForOptimizer();
        newExecutionContext.getExtraCmds().put(ConnectionProperties.ENABLE_INDEX_SELECTION, false);
        ExecutionPlan newExecutionPlan = Planner.getInstance().doBuildPlan(sqlParameterized, newExecutionContext);
        // HotGsiPattern shouldn't find gsi here, except that force index(gsi) is used or select gsi table directly
        // in which case, we don't evolve
        if (HotGsiPattern.findPattern(newExecutionPlan.getPlan()).getKey()) {
            return false;
        }
        addNewPlanInfo(executionContext, executionPlan, baselineInfo, info);

        baselineInfo.setHotEvolution(true);
        FeatureStats.getInstance().increment(HOT_EVOLVE_PLAN_NUM);
        return addNewPlanInfo(newExecutionContext, newExecutionPlan, baselineInfo, info);
    }

    /**
     * @return true if new plan is added
     */
    private static boolean addNewPlanInfo(ExecutionContext executionContext,
                                          ExecutionPlan executionPlan,
                                          BaselineInfo baselineInfo,
                                          GsiEvolutionInfo info) {
        PlanManager planManager = PlanManager.getInstance();

        // don't add if there are too many accepted plans
        if (baselineInfo.getAcceptedPlans().size() >= executionContext.getParamManager()
            .getInt(ConnectionParams.SPM_MAX_ACCEPTED_PLAN_SIZE_PER_BASELINE)) {
            LoggerUtil.logSpm(executionContext.getSchemaName(),
                String.format("HGE: schema [%s] traceid [%s] tid [%s] fails to evolve due to baseline plan full",
                    executionContext.getSchemaName(), executionContext.getTraceId(), info.getTemplateId()));
            return false;
        }
        if (!PlanManagerUtil.baselineSupported(executionPlan.getPlan())) {
            LoggerUtil.logSpm(executionContext.getSchemaName(),
                String.format("HGE: schema [%s] traceid [%s] tid [%s]  fails to evolve due to plan not supported",
                    executionContext.getSchemaName(), executionContext.getTraceId(), info.getTemplateId()));
            return false;
        }
        RelNode plan = executionPlan.getPlan();
        String planJsonString = PlanManagerUtil.relNodeToJson(plan);
        PlanInfo planInfo =
            planManager.createPlanInfo(
                executionContext.getSchemaName(), planJsonString, plan, baselineInfo.getId(),
                executionContext.getTraceId(),
                PlanManagerUtil.getPlanOrigin(plan), executionPlan.getAst(), executionContext);
        // set LastExecuteTime to avoid bing removed directly
        planInfo.setLastExecuteTime(GeneralUtil.unixTimeStamp());
        if (baselineInfo.addAcceptedPlan(planInfo) == null) {
            LoggerUtil.logSpm(executionContext.getSchemaName(),
                String.format("HGE: generate spm baseline_id[%s] plan_id[%s] for trace_id [%s] tid[%s]",
                    baselineInfo.getId(), planInfo.getId(), executionContext.getTraceId(), info.getTemplateId()));
            return true;
        }
        return false;
    }

}
