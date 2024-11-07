package com.alibaba.polardbx.planner.oss;

import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.junit.runners.Parameterized;

import java.util.List;

public class Tpch1TColumnarModeTest extends ParameterizedTestCommon {
    public Tpch1TColumnarModeTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
        //setExplainCost(true);
    }

    @Override
    protected String getPlan(String testSql) {
        try {
            ConfigDataMode.setInstanceRole(InstanceRole.COLUMNAR_SLAVE);
            return super.getPlan(testSql);
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(Tpch1TColumnarModeTest.class);
    }
}
