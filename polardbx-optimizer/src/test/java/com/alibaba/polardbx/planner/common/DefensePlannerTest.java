package com.alibaba.polardbx.planner.common;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.TStringUtil;

import java.util.Locale;

public abstract class DefensePlannerTest extends PlanTestCommon {
    public DefensePlannerTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        // TODO Auto-generated constructor stub
    }

    protected void execSqlAndVerifyPlan(String testMethodName, Integer sqlIdx, String targetSql, String exceptErrorMsg,
                                        String except, String nodetree) {
        try {
            getPlan(targetSql);
        } catch (Throwable e) {
            String err = e.getMessage().toLowerCase(Locale.ROOT);
            if (exceptErrorMsg == null || TStringUtil.isBlank(exceptErrorMsg)) {
                return;
            }
            if (TStringUtil.isBlank(err) || !err.contains(exceptErrorMsg.trim().toLowerCase(Locale.ROOT))) {
                Assert.fail(String.format("plan failed, but reason not matched, expect %s, but was %s, sql is %s",
                    exceptErrorMsg, err, targetSql));
            }
            // case failed with expect error message
            return;
        }
        Assert.fail("plan should fail, but not, sql is : " + targetSql);
    }
}