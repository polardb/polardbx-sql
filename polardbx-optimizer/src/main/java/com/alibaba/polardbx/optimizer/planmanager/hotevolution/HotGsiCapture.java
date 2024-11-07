package com.alibaba.polardbx.optimizer.planmanager.hotevolution;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertManager;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang.StringUtils;

import java.util.Locale;

import static org.apache.calcite.sql.SqlKind.QUERY;

/**
 * try to capture plan with hot gsi
 */
public class HotGsiCapture {

    /**
     * entry point capture plan with hot gsi
     *
     * @param sqlParameterized origin sql
     * @param ec context of the sql
     * @param executionPlan current plan to check
     * @param hotGsiEvolution hot gsi evolution
     */
    public static boolean capture(
        SqlParameterized sqlParameterized,
        ExecutionContext ec,
        ExecutionPlan executionPlan,
        HotGsiEvolution hotGsiEvolution,
        String templateId) {
        String schema = ec.getSchemaName();

        try {
            // pre-test
            if (!preTest(sqlParameterized, ec, executionPlan)) {
                return false;
            }

            // capture hot gsi
            String index = candidateHotGsi(ec, executionPlan.getPlan());

            if (!StringUtils.isEmpty(index)) {
                // submit to evolution
                boolean offer = hotGsiEvolution.submitEvolutionTask(
                    new GsiEvolutionInfo(sqlParameterized, ec, executionPlan, index, templateId));
                if (!offer) {
                    LoggerUtil.logSpm(schema,
                        String.format(
                            "HGE: schema [%s] traceid [%s] tid [%s] fails to capture due to queue full[%d]",
                            ec.getSchemaName(),
                            ec.getTraceId(),
                            templateId,
                            hotGsiEvolution.getQueueSize()));
                    return false;
                }
                LoggerUtil.logSpm(schema,
                    String.format("HGE: schema [%s] traceid [%s] tid [%s] is submitted to evolution",
                        ec.getSchemaName(),
                        ec.getTraceId(),
                        templateId));
                return true;
            }
            LoggerUtil.logSpm(schema,
                String.format(
                    "HGE: schema [%s] traceid [%s] tid [%s] fails to capture due to can't find any hot gsi",
                    ec.getSchemaName(),
                    ec.getTraceId(),
                    templateId));
        } catch (Throwable e) {
            LoggerUtil.logSpm(schema, e.getMessage());
        }
        return false;
    }

    /**
     * check whether the plan matches specific pattern and contains hot gsi
     *
     * @param ec context
     * @param plan the plan to be checked
     * @return gsi name if the plan contains hot gsi, null if not found
     */
    public static String candidateHotGsi(ExecutionContext ec, RelNode plan) {
        // check if the plan matches specific pattern
        Pair<Boolean, LogicalIndexScan> getHotGsiPattern = HotGsiPattern.findPattern(plan);
        if (!getHotGsiPattern.getKey()) {
            return null;
        }
        LogicalIndexScan indexScan = getHotGsiPattern.getValue();
        String indexName = indexScan.getTableNames().get(0);
        // don't evolute if statistics missed
        if (StatisticManager.expired(ec.getSchemaName(), indexName)) {
            OptimizerAlertManager.getInstance().log(OptimizerAlertType.STATISTIC_MISS, ec);
            return null;
        }

        // calculate max selectivity
        double rowCount = CBOUtil.getTableMeta(indexScan.getTable()).getRowCount(null) *
            indexScan.getMaxSelectivity();
        if (rowCount > ec.getParamManager().getLong(ConnectionParams.HOT_GSI_EVOLUTION_THRESHOLD)) {
            return indexName;
        }
        return null;
    }

    /**
     * pre-test the plan, prevent call candidateHotGsi if preTest fails
     *
     * @param sqlParameterized origin sql
     * @param ec context of the sql
     * @param executionPlan current plan to check
     * @return true if pre-test passed
     */
    private static boolean preTest(
        SqlParameterized sqlParameterized,
        ExecutionContext ec,
        ExecutionPlan executionPlan) {
        // should enable hot evolution
        if (!DynamicConfig.getInstance().enableHotGsiEvolution()) {
            return false;
        }
        // should enable index selection
        if (!ec.getParamManager().getBoolean(ConnectionParams.ENABLE_INDEX_SELECTION)) {
            return false;
        }
        String schema = ec.getSchemaName();
        if (StringUtils.isEmpty(schema)) {
            return false;
        }
        schema = schema.toLowerCase(Locale.ROOT);
        // don't evolve built-in-database
        if (SystemDbHelper.isDBBuildIn(schema)) {
            return false;
        }
        // should use spm
        if (!PlanManagerUtil.useSpm(sqlParameterized, ec)) {
            return false;
        }

        // don't evolve if the sql is too long
        if (sqlParameterized.getSql().length() >
            ec.getParamManager().getInt(ConnectionParams.SPM_MAX_BASELINE_INFO_SQL_LENGTH)) {
            return false;
        }
        // only consider query
        if (!executionPlan.getAst().isA(QUERY)) {
            return false;
        }

        // don't evolve if plan cache is full
        if (PlanCache.getInstance().getCache().size() >= PlanCache.getInstance().getCurrentCapacity() - 20) {
            return false;
        }

        // don't evolve if spm is full
        if (!PlanManager.getInstance().baselineSizeCheck(schema)) {
            return false;
        }

        // don't evolve sql with index hint
        if (executionPlan.checkProperty(ExecutionPlanProperties.WITH_INDEX_HINT)) {
            return false;
        }
        // don't evolve if the baseline can't evolve
        BaselineInfo baselineInfo =
            PlanManager.getInstance().getBaselineMap(schema).get(sqlParameterized.getSql());
        if (baselineInfo != null && (!baselineInfo.canHotEvolution())) {
            return false;
        }

        // don't evolve sql with join
        if (PlannerContext.getPlannerContext(executionPlan.getPlan()).getJoinCount() > 0) {
            return false;
        }
        // don't evolve sql with subquery
        if (OptimizerUtils.hasSubquery(executionPlan.getPlan())) {
            return false;
        }

        return true;
    }
}
