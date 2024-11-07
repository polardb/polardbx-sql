package com.alibaba.polardbx.planner.rewriter;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.Xplan.XPlanTemplate;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.RelToXPlanConverter;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.RelXPlanOptimizer;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.planner.common.EclipseParameterized;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;

@RunWith(EclipseParameterized.class)
public class XplanSkewTest extends PlanTestCommon {

    public XplanSkewTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(XplanSkewTest.class);
    }

    @Override
    protected String getPlan(String testSql) {

        ExecutionContext executionContext = new ExecutionContext(appName);
        executionContext.setParams(new Parameters());
        SqlNode ast = parserSqlNode(executionContext, testSql);

        this.cluster =
            SqlConverter.getInstance(executionContext).createRelOptCluster(PlannerContext.EMPTY_CONTEXT);

        final HintPlanner hintPlanner = HintPlanner.getInstance(appName, executionContext);
        executionContext.setInternalSystemSql(false);
        executionContext.setUsingPhySqlCache(true);
        executionContext.getExtraCmds().putAll(configMaps);

        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(appName, executionContext.getExtraCmds(),
            executionContext.getGroupHint());

        executionContext.setServerVariables(new HashMap<>());
        hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);
        // reset cluster for every statement
        ExecutionPlan executionPlan = Planner.getInstance().getPlan(ast, plannerContext);

        RelNode leaf = executionPlan.getPlan();
        while (!(leaf instanceof LogicalView)) {
            leaf = leaf.getInput(0);
        }
        LogicalView lv = (LogicalView) leaf;
        final RelNode pushedRel = lv.getPushedRelNode();
        final RelToXPlanConverter converter = new RelToXPlanConverter();
        XPlanTemplate xplan = null;
        try {
            xplan = converter.convert(RelXPlanOptimizer.optimize(pushedRel));
        } catch (Exception e) {
            Throwable throwable = e;
            while (throwable.getCause() != null && throwable.getCause() instanceof InvocationTargetException) {
                throwable = ((InvocationTargetException) throwable.getCause()).getTargetException();
            }
        }

        return returnXPlanStr(xplan);

    }

    protected String returnXPlanStr(XPlanTemplate xplan) {
        if (xplan == null) {
            return "null";
        }
        return xplan.toString();
    }
}
