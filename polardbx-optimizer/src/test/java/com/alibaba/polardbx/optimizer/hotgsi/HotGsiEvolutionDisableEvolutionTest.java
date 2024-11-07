package com.alibaba.polardbx.optimizer.hotgsi;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * test ENABLE_HOT_GSI_EVOLUTION of HotGsiEvolution
 */
public class HotGsiEvolutionDisableEvolutionTest extends HotGsiEvolutionTest {
    public HotGsiEvolutionDisableEvolutionTest(String caseName, int sqlIndex, String sql,
                                               String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(HotGsiEvolutionDisableEvolutionTest.class);
    }

    protected String validator(SqlParameterized sqlParameterized, ExecutionPlan executionPlan,
                               ExecutionContext executionContext) {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        DynamicConfig.getInstance()
            .loadValue(null, ConnectionProperties.ENABLE_HOT_GSI_EVOLUTION, String.valueOf(false));
        PlanManager.getInstance().getBaselineMap().clear();
        invokeEvolve(sqlParameterized, executionPlan, executionContext);
        Assert.assertTrue(PlanManager.getInstance().getBaselineMap(appName).isEmpty());

        DynamicConfig.getInstance()
            .loadValue(null, ConnectionProperties.ENABLE_HOT_GSI_EVOLUTION, String.valueOf(true));
        PlanManager.getInstance().getBaselineMap().clear();
        invokeEvolve(sqlParameterized, executionPlan, executionContext);
        Assert.assertTrue(PlanManager.getInstance().getBaselineMap(appName).size() == 1);
        String planStr = RelUtils
            .toString(executionPlan.getPlan(), OptimizerUtils.buildParam(sqlParameterized.getParameters()),
                RexUtils.getEvalFunc(executionContext), executionContext);

        String code = removeSubqueryHashCode(planStr, executionPlan.getPlan(),
            executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter(),
            executionContext.getSqlExplainLevel());
        return code;
    }
}
