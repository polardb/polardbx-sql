package com.alibaba.polardbx.planner.rewriter;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.rule.ConstantFoldRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.ProjectFoldRule;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import com.alibaba.polardbx.planner.common.EclipseParameterized;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(EclipseParameterized.class)
public class ConstantFoldRuleTest extends BasePlannerTest {

    public ConstantFoldRuleTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(ConstantFoldRuleTest.class);
    }

    @Override
    protected String getPlan(String testSql) {
        Planner planner = new RewriterPlanner();
        FastsqlParser fastSqlParser = new FastsqlParser();
        SqlNodeList astList = fastSqlParser.parse(testSql);

        SqlNode ast = astList.get(0);

        ExecutionContext executionContext = new ExecutionContext();
        final HintPlanner hintPlanner = HintPlanner.getInstance(appName, executionContext);
        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(appName, executionContext.getExtraCmds(),
            executionContext.getGroupHint());

        executionContext.setParams(new Parameters());
        enableExplainCost(executionContext);
        hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);

        ExecutionPlan executionPlan = planner.getPlan(ast, plannerContext);
        RelDrdsWriter relWriter = new RelDrdsWriter();
        executionPlan.getPlan().explainForDisplay(relWriter);

        String planStr = RelUtils.toString(executionPlan.getPlan(), null, null, executionContext);

        return removeSubqueryHashCode(planStr, executionPlan.getPlan(), null);
    }

    protected void enableExplainCost(ExecutionContext executionContext) {
    }

    protected static class RewriterPlanner extends Planner {
        @Override
        public RelNode optimize(RelNode input, PlannerContext plannerContext) {
            HepProgramBuilder hepPgmBuilder = new HepProgramBuilder();
            hepPgmBuilder.addMatchOrder(HepMatchOrder.BOTTOM_UP);

            hepPgmBuilder.addGroupBegin();
            hepPgmBuilder.addRuleInstance(ConstantFoldRule.INSTANCE);
            hepPgmBuilder.addRuleInstance(ProjectFoldRule.INSTANCE);
            hepPgmBuilder.addGroupEnd();
            final HepPlanner planner = new HepPlanner(hepPgmBuilder.build(), plannerContext);
            planner.setRoot(input);
            return planner.findBestExp();
        }
    }
}
