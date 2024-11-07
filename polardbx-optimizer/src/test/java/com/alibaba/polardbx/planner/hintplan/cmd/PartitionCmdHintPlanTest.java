package com.alibaba.polardbx.planner.hintplan.cmd;

import com.alibaba.polardbx.planner.common.EclipseParameterized;
import com.alibaba.polardbx.planner.hintplan.index.ParameterizedHintTestCommon;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(EclipseParameterized.class)
public class PartitionCmdHintPlanTest extends ParameterizedHintTestCommon {
    public PartitionCmdHintPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(PartitionCmdHintPlanTest.class);
    }
}
