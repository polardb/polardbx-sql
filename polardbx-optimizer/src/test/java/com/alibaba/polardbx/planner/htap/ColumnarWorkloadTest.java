package com.alibaba.polardbx.planner.htap;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.planner.common.ParameterizedTestCommon;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Map;

public class ColumnarWorkloadTest extends ParameterizedTestCommon {
    public ColumnarWorkloadTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(ColumnarWorkloadTest.class);
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

    @Override
    public String removeSubqueryHashCode(String planStr, RelNode plan, Map<Integer, ParameterContext> param,
                                         SqlExplainLevel sqlExplainLevel) {
        return PlannerContext.getPlannerContext(plan).getWorkloadType().toString();
    }
}
