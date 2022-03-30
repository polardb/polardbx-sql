package com.alibaba.polardbx.planner.rewriter;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * optimize logicalView's relNode tree, simplify the sql sent to DN.
 * unwrap subQueries by making join operations adjacent in the tree
 *
 * @author shengyu
 */
public class LogicalViewOptTest extends PlanTestCommon {
    public LogicalViewOptTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(LogicalViewOptTest.class);
    }
}