package com.alibaba.polardbx.optimizer.hotgsi;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.LogicalViewFinder;
import com.alibaba.polardbx.planner.common.HotGsiTestCommon;
import org.apache.calcite.rel.RelNode;
import org.junit.runners.Parameterized;

import java.util.List;

public class HotGsiSelectivityUpperLimitEstimatorTest extends HotGsiTestCommon {
    public HotGsiSelectivityUpperLimitEstimatorTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                                    String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(HotGsiSelectivityUpperLimitEstimatorTest.class);
    }

    @Override
    protected String validator(SqlParameterized sqlParameterized, ExecutionPlan executionPlan,
                               ExecutionContext executionContext) {
        RelNode node = executionPlan.getPlan();

        LogicalViewFinder finder = new LogicalViewFinder();
        node.accept(finder);
        LogicalView lv = finder.getResult().get(0);
        lv.getPushDownOpt().optimize();
        double result = lv.getMaxSelectivity() *
            CBOUtil.getTableMeta(lv.getTable()).getRowCount(null);

        return String.format("%.0f", result);
    }
}