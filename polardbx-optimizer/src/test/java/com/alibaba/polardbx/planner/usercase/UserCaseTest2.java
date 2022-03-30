package com.alibaba.polardbx.planner.usercase;

import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author Shi Yuxuan
 */
public class UserCaseTest2 extends PlanTestCommon {

    public UserCaseTest2(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(UserCaseTest2.class);
    }

}