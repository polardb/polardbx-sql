package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckerInput;
import org.junit.Test;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_COLUMNAR_OPTIMIZER;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_COLUMNAR_OPTIMIZER_WITH_COLUMNAR;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_MPP;
import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static com.alibaba.polardbx.druid.sql.ast.SqlType.SELECT;
import static com.alibaba.polardbx.druid.sql.ast.SqlType.SELECT_FOR_UPDATE;
import static com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckers.COLUMNAR_TRANSACTION_CHECKER;
import static com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckers.ENABLE_COLUMNAR_CHECKER;
import static com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckers.EXPLAIN_EXECUTE_CHECKER;
import static com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckers.EXPLAIN_STATISTICS_CHECKER;
import static com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckers.INTERNAL_SYSTEM_SQL_CHECKER;
import static com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckers.MPP_ENABLED_CHECKER;
import static com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckers.TRANSACTION_CHECKER;
import static com.alibaba.polardbx.optimizer.utils.mppchecker.MppPlanCheckers.UPDATE_CHECKER;

/**
 * @author fangwu
 */
public class MppPlanCheckersTest {

    @Test
    public void testMppEnable() {
        PlannerContext plannerContext = new PlannerContext();
        ExecutionContext ec1 = new ExecutionContext();
        ec1.getParamManager().getProps().put(ENABLE_MPP, "false");

        plannerContext.setExecutionContext(ec1);

        ExecutionContext ec2 = new ExecutionContext();
        ec2.getParamManager().getProps().put(ENABLE_MPP, "true");

        MppPlanCheckerInput input = new MppPlanCheckerInput(null, plannerContext, ec2);

        assertTrue(MPP_ENABLED_CHECKER.supportsMpp(input));

        ec2.getParamManager().getProps().put(ENABLE_MPP, "false");

        assertTrue(!MPP_ENABLED_CHECKER.supportsMpp(input));
    }

    @Test
    public void testTransaction() {
        PlannerContext plannerContext = new PlannerContext();
        ExecutionContext ec1 = new ExecutionContext();
        ec1.setAutoCommit(false);
        plannerContext.setExecutionContext(ec1);

        ExecutionContext ec2 = new ExecutionContext();
        ec2.setAutoCommit(true);

        MppPlanCheckerInput input = new MppPlanCheckerInput(null, plannerContext, ec2);

        assertTrue(TRANSACTION_CHECKER.supportsMpp(input));

        ec2.setAutoCommit(false);

        assertTrue(!TRANSACTION_CHECKER.supportsMpp(input));
    }

    @Test
    public void testColumnarTransaction() {
        PlannerContext plannerContext = new PlannerContext();
        ExecutionContext ec1 = new ExecutionContext();
        ec1.setAutoCommit(false);
        plannerContext.setExecutionContext(ec1);

        ExecutionContext ec2 = new ExecutionContext();
        ec2.setAutoCommit(true);

        MppPlanCheckerInput input = new MppPlanCheckerInput(null, plannerContext, ec2);

        assertTrue(COLUMNAR_TRANSACTION_CHECKER.supportsMpp(input));

        ec2.setAutoCommit(false);

        assertTrue(!COLUMNAR_TRANSACTION_CHECKER.supportsMpp(input));
    }

    @Test
    public void testUpdateChecker() {
        PlannerContext plannerContext = new PlannerContext();
        ExecutionContext ec1 = new ExecutionContext();
        ec1.setSqlType(SELECT_FOR_UPDATE);
        plannerContext.setExecutionContext(ec1);

        ExecutionContext ec2 = new ExecutionContext();
        ec2.setSqlType(SELECT_FOR_UPDATE);

        MppPlanCheckerInput input = new MppPlanCheckerInput(null, plannerContext, ec2);

        assertTrue(!UPDATE_CHECKER.supportsMpp(input));

        ec2.setSqlType(SELECT);

        assertTrue(UPDATE_CHECKER.supportsMpp(input));
    }

    @Test
    public void testInternalSystemSqlChecker() {
        PlannerContext plannerContext = new PlannerContext();
        ExecutionContext ec1 = new ExecutionContext();
        ec1.setInternalSystemSql(true);
        plannerContext.setExecutionContext(ec1);

        ExecutionContext ec2 = new ExecutionContext();
        ec2.setInternalSystemSql(true);

        MppPlanCheckerInput input = new MppPlanCheckerInput(null, plannerContext, ec2);

        assertTrue(!INTERNAL_SYSTEM_SQL_CHECKER.supportsMpp(input));

        ec2.setInternalSystemSql(false);

        assertTrue(INTERNAL_SYSTEM_SQL_CHECKER.supportsMpp(input));
    }

    @Test
    public void testExplainExecuteChecker() {
        PlannerContext plannerContext = new PlannerContext();
        ExecutionContext ec1 = new ExecutionContext();
        ec1.setExplain(new ExplainResult());
        ec1.getExplain().explainMode = ExplainResult.ExplainMode.EXECUTE;
        plannerContext.setExecutionContext(ec1);

        ExecutionContext ec2 = new ExecutionContext();
        ec2.setExplain(new ExplainResult());
        ec2.getExplain().explainMode = ExplainResult.ExplainMode.OPTIMIZER;

        MppPlanCheckerInput input = new MppPlanCheckerInput(null, plannerContext, ec2);

        assertTrue(EXPLAIN_EXECUTE_CHECKER.supportsMpp(input));

        ec2.getExplain().explainMode = ExplainResult.ExplainMode.EXECUTE;

        assertTrue(!EXPLAIN_EXECUTE_CHECKER.supportsMpp(input));
    }

    @Test
    public void testExplainStatisticsChecker() {
        PlannerContext plannerContext = new PlannerContext();
        ExecutionContext ec1 = new ExecutionContext();
        ec1.setExplain(new ExplainResult());
        ec1.getExplain().explainMode = ExplainResult.ExplainMode.STATISTICS;
        plannerContext.setExecutionContext(ec1);

        ExecutionContext ec2 = new ExecutionContext();
        ec2.setExplain(new ExplainResult());
        ec2.getExplain().explainMode = ExplainResult.ExplainMode.OPTIMIZER;

        MppPlanCheckerInput input = new MppPlanCheckerInput(null, plannerContext, ec2);

        assertTrue(EXPLAIN_STATISTICS_CHECKER.supportsMpp(input));

        ec2.getExplain().explainMode = ExplainResult.ExplainMode.STATISTICS;

        assertTrue(!EXPLAIN_STATISTICS_CHECKER.supportsMpp(input));
    }

    @Test
    public void testEnableColumnarChecker() {
        PlannerContext plannerContext = new PlannerContext();
        ExecutionContext ec1 = new ExecutionContext();
        ec1.getParamManager().getProps().put(ENABLE_COLUMNAR_OPTIMIZER, "false");

        plannerContext.setExecutionContext(ec1);

        ExecutionContext ec2 = new ExecutionContext();
        ec2.getParamManager().getProps().put(ENABLE_COLUMNAR_OPTIMIZER, "true");

        MppPlanCheckerInput input = new MppPlanCheckerInput(null, plannerContext, ec2);

        assertTrue(ENABLE_COLUMNAR_CHECKER.supportsMpp(input));

        ec2.getParamManager().getProps().put(ENABLE_COLUMNAR_OPTIMIZER, "false");

        assertTrue(!ENABLE_COLUMNAR_CHECKER.supportsMpp(input));

        ec2.getParamManager().getProps().put(ENABLE_COLUMNAR_OPTIMIZER, "false");

        boolean orig = DynamicConfig.getInstance().existColumnarNodes();
        try {
            ec2.getParamManager().getProps().put(ENABLE_COLUMNAR_OPTIMIZER_WITH_COLUMNAR, "true");
            DynamicConfig.getInstance().existColumnarNodes(true);
            assertTrue(ENABLE_COLUMNAR_CHECKER.supportsMpp(input));
            ec2.getParamManager().getProps().put(ENABLE_COLUMNAR_OPTIMIZER_WITH_COLUMNAR, "false");
            assertTrue(!ENABLE_COLUMNAR_CHECKER.supportsMpp(input));
            ec2.getParamManager().getProps().put(ENABLE_COLUMNAR_OPTIMIZER_WITH_COLUMNAR, "true");
            DynamicConfig.getInstance().existColumnarNodes(false);
            assertTrue(!ENABLE_COLUMNAR_CHECKER.supportsMpp(input));
        } finally {
            DynamicConfig.getInstance().existColumnarNodes(orig);
        }
    }
}
