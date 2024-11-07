package com.alibaba.polardbx.planner.common;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.PostPlanner;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class HotGsiTestCommon extends ParameterizedTestCommon {
    public HotGsiTestCommon(String caseName, int sqlIndex, String sql, String expectedPlan,
                            String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Override
    protected String getPlan(String testSql) {
        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setServerVariables(new HashMap<>());
        executionContext.setAppName(appName);
        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(
            ByteString.from(testSql), currentParameter, executionContext, false);
        setSysDefVariable(sqlParameterized.getParameters());
        Map<Integer, ParameterContext> param = OptimizerUtils.buildParam(sqlParameterized.getParameters());
        SqlNodeList astList = new FastsqlParser().parse(
            sqlParameterized.getSql(), sqlParameterized.getParameters(), executionContext);
        SqlNode ast = astList.get(0);

        Set<Pair<String, String>> tableSet = PlanManagerUtil.getTableSetFromAst(ast);
        final HintPlanner hintPlanner = HintPlanner.getInstance(appName, executionContext);
        executionContext.setParams(new Parameters(param, false));
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_AUTO_FORCE_INDEX, enableAutoForceIndex);
        executionContext.getExtraCmds().putAll(configMaps);
        final HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean(appName,
            executionContext.getExtraCmds(),
            executionContext.getGroupHint());

        hintPlanner.collectAndPreExecute(ast, cmdBean, false, executionContext);
        processParameter(sqlParameterized, executionContext);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);

        ExecutionPlan executionPlan = Planner.getInstance().getPlan(ast, plannerContext);
        executionPlan = PostPlanner.getInstance().optimize(executionPlan, executionContext);

        int tablesVersion = PlanManagerUtil.computeTablesVersion(tableSet, appName, executionContext);
        Map<String, TableMeta> tableMetaSet = PlanManagerUtil.getTableMetaSetByTableSet(tableSet, executionContext);
        executionPlan.saveCacheState(tableSet, tablesVersion, null, tableMetaSet);

        return validator(sqlParameterized, executionPlan, executionContext);
    }

    protected String validator(SqlParameterized sqlParameterized, ExecutionPlan executionPlan,
                               ExecutionContext executionContext) {
        return "";
    }
}