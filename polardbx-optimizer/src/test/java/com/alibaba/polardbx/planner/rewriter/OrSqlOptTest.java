package com.alibaba.polardbx.planner.rewriter;

import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * optimize 'or', simplify the sql.
 * 1.compress 'or' to 'in', 2.transform left-deep tree to balanced tree 3.flatten sqlNode tree to list
 *
 * @author shengyu
 */
public class OrSqlOptTest extends ParameterizedTestCommon {
    public OrSqlOptTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(OrSqlOptTest.class);
    }
}