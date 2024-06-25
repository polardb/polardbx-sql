package com.alibaba.polardbx.planner.ddl;

import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class AlterTableSplitPartitionTest extends ParameterizedTestCommon {

    public AlterTableSplitPartitionTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                        String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(AlterTableSplitPartitionTest.class);
    }

    public void testAlterAstToString() {
        try {

        } catch (Throwable e) {

        }
    }
}