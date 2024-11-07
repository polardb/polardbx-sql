package com.alibaba.polardbx.planner.usercase;

import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author fangwu
 */
public class UserCaseAutoTest extends ParameterizedTestCommon {

    public UserCaseAutoTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(UserCaseAutoTest.class);
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

}