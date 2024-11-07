/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.executor.PlanExecutor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.planmanagement.BaselineSyncController;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.BaselineDeleteHotEvolvedSyncAction;
import com.alibaba.polardbx.executor.sync.BaselineLoadSyncAction;
import com.alibaba.polardbx.executor.sync.BaselinePersistSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.executor.utils.ExplainExecutorUtil;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalBaseline;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.workload.WorkloadType;
import com.alibaba.polardbx.stats.metric.FeatureStats;
import com.alibaba.polardbx.stats.metric.FeatureStatsItem;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlBaseline;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_BASELINE;
import static com.alibaba.polardbx.stats.metric.FeatureStatsItem.FIX_PLAN_NUM;

public class LogicalBaselineHandler extends HandlerCommon {

    public LogicalBaselineHandler(IRepository repo) {
        super(repo);
    }

    public enum BASELINE_OPERATION {
        LIST("List all baseline infos", "baseline list"),
        ADD_FIX("Make plan modified by HINT externalized in baseline",
            "BASELINE (ADD|FIX) SQL <HINT> <Select Statement>"),
        SELECT("show baseline based on the sql",
            "BASELINE SELECT SQL <HINT> <Select Statement>"),
        FEEDBACK_WORKLOAD("feedback workload based on the sql",
            "BASELINE FEEDBACK_WORKLOAD SQL <HINT> <Select Statement>"),
        LOAD("Load all baseline info from disk to RAM", "BASELINE LOAD <baseline id>"),
        PERSIST("Write the specified baseline into disk", "BASELINE PERSIST <baseline id>"),
        VALIDATE("Check baseline", "BASELINE VALIDATE <baseline id>"),
        DELETE("Delete baseline in disk by baseline id", "BASELINE DELETE <baseline id>"),
        DELETE_ALL("Delete baseline in disk", "BASELINE DELETE_ALL>"),
        DELETE_PLAN("Delete plan in disk by plan id", "BASELINE DELETE_PLAN <plan id>"),
        DELETE_EVOLVED("Delete baseline with hot gsi evolved", "BASELINE DELETE_EVOLVED"),
        HELP("baseline manual", "BASELINE HELP");

        private String desc;
        private String example;

        BASELINE_OPERATION(String desc, String example) {
            this.desc = desc;
            this.example = example;
        }

        public String getDesc() {
            return desc;
        }

        public String getExample() {
            return example;
        }
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        assert logicalPlan instanceof LogicalBaseline;
        LogicalBaseline logicalBaseline = (LogicalBaseline) logicalPlan;
        SqlBaseline sqlBaseline = logicalBaseline.getSqlBaseline();
        List<Long> baselineIds = sqlBaseline.getBaselineIds();
        String schema = executionContext.getSchemaName();
        switch (sqlBaseline.getOperation().toUpperCase()) {
        case "LIST": {
            return baselineList(baselineIds, executionContext, logicalPlan.getCluster());
        }

        case "SELECT": {
            String parameterizedSql = sqlBaseline.getParameterizedSql();
            Map<String, BaselineInfo> baselineInfoMap = PlanManager.getInstance().getBaselineMap(schema);
            BaselineInfo baselineInfo = baselineInfoMap.get(parameterizedSql);
            baselineIds = new ArrayList<>();
            if (baselineInfo != null) {
                baselineIds.add((long) baselineInfo.getId());
                return baselineList(baselineIds, executionContext, logicalPlan.getCluster());
            } else {
                throw new TddlRuntimeException(ERR_BASELINE, String.format("unknown sql!"));
            }
        }

        case "FEEDBACK_WORKLOAD": {
            String parameterizedSql = sqlBaseline.getParameterizedSql();
            ExecutionContext newContext = executionContext.copy();
            PlanExecutor planExecutor = new PlanExecutor();
            planExecutor.init();
            ExecutionPlan originExecutionPlan = Planner.getInstance().plan(parameterizedSql, newContext);
            PlannerContext plannerContext = PlannerContext.getPlannerContext(originExecutionPlan.getPlan());
            WorkloadType workloadType = plannerContext.getWorkloadType();
            WorkloadType feedBackWorkload = null;
            if (workloadType == WorkloadType.AP) {
                feedBackWorkload = WorkloadType.TP;
            } else if (workloadType == WorkloadType.TP) {
                feedBackWorkload = WorkloadType.AP;
            }
            Map<String, BaselineInfo> baselineInfoMap = PlanManager.getInstance().getBaselineMap(schema);
            BaselineInfo baselineInfo = baselineInfoMap.get(parameterizedSql);

            if (feedBackWorkload != null && baselineInfo != null && plannerContext.getPlanInfo() != null) {
                PlanManager.getInstance()
                    .notifyUpdatePlanSync(originExecutionPlan, baselineInfo, plannerContext.getPlanInfo(),
                        feedBackWorkload, newContext);
                baselineIds = new ArrayList<>();
                baselineInfo = baselineInfoMap.get(parameterizedSql);
                if (baselineInfo != null) {
                    baselineIds.add((long) baselineInfo.getId());
                }
                return baselineList(baselineIds, executionContext, logicalPlan.getCluster());
            } else {
                throw new TddlRuntimeException(ERR_BASELINE, String.format("unknown workload or plan_info is null!"));
            }
        }

        case "ADD": {
            String hint = sqlBaseline.getHint();
            String parameterizedSql = sqlBaseline.getParameterizedSql();
            return baselineAdd(hint, parameterizedSql, executionContext, false, false, Planner.getInstance(),
                PlanManager.getInstance());
        }
        case "HINT": {
            String hint = sqlBaseline.getHint();
            String parameterizedSql = sqlBaseline.getParameterizedSql();
            return baselineAdd(hint, parameterizedSql, executionContext, true, true, Planner.getInstance(),
                PlanManager.getInstance());
        }
        case "FIX": {
            String hint = sqlBaseline.getHint();
            String parameterizedSql = sqlBaseline.getParameterizedSql();
            return baselineAdd(hint, parameterizedSql, executionContext, true, false, Planner.getInstance(),
                PlanManager.getInstance());
        }

        case "LOAD":
        case "PERSIST":
        case "VALIDATE":
        case "DELETE":
        case "DELETE_ALL":
        case "DELETE_PLAN":
        case "DELETE_EVOLVED": {
            return baselineLPCVD(baselineIds, executionContext, sqlBaseline.getOperation(), PlanManager.getInstance());
        }
        case "HELP": {
            return baselineHelp();
        }
        default:
            throw new TddlRuntimeException(ERR_BASELINE,
                String.format("not support baseline %s statement", sqlBaseline.getOperation().toUpperCase()));
        }
    }

    private Cursor baselineHelp() {
        ArrayResultCursor result = new ArrayResultCursor("baseline");
        result.addColumn("STATEMENT", DataTypes.StringType);
        result.addColumn("DESCRIPTION", DataTypes.StringType);
        result.addColumn("EXAMPLE", DataTypes.StringType);

        for (BASELINE_OPERATION op : BASELINE_OPERATION.values()) {
            Object[] row = new Object[3];
            row[0] = op.name();
            row[1] = op.desc;
            row[2] = op.example;
            result.addRow(row);
        }
        return result;
    }

    protected Cursor baselineAdd(String hint, String parameterizedSql, ExecutionContext executionContext, boolean fix,
                                 boolean hintBind, Planner planner, PlanManager planManager) {
        String schemaName = executionContext.getSchemaName();
        ArrayResultCursor result = new ArrayResultCursor("baseline");
        result.addColumn("BASELINE_ID", DataTypes.IntegerType);
        result.addColumn("PLAN_ID", DataTypes.IntegerType);
        result.addColumn("STATUS", DataTypes.StringType);
        result.addColumn("PLAN", DataTypes.StringType);
        if (hint == null) {
            throw new TddlRuntimeException(ERR_BASELINE, "not support baseline add statement without hint");
        }
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.init();

        ExecutionPlan executionPlan = planner.plan(hint + " " + parameterizedSql, executionContext);
        if (executionPlan.getConstantParams() != null) {
            throw new TddlRuntimeException(ERR_BASELINE,
                "not support baseline add plan with generated column substitution");
        }

        Map<String, BaselineInfo> baselineInfoMap = planManager.getBaselineMap(schemaName);
        BaselineInfo baselineInfo = baselineInfoMap.get(parameterizedSql);
        RelNode plan = executionPlan.getPlan();
        SqlNode ast = executionPlan.getAst();
        boolean withPushdownHint = Optional
            .ofNullable(executionPlan.getHintCollection())
            .map(HintConverter.HintCollection::pushdownSqlOrRoute)
            .orElse(false);
        if (baselineInfo == null) {
            baselineInfo = planManager.createBaselineInfo(parameterizedSql, ast, executionContext);
        } else if (withPushdownHint || hintBind) {
            // For one logical sql exists at most one baseline with pushdown hint
            result.addRow(new Object[] {baselineInfo.getId(), null, "ExecutionPlan exists", null});
            return result;
        }

        if (hintBind) {
            baselineInfo.setHint(hint);
            baselineInfo.setRebuildAtLoad(true);
            baselineInfoMap.put(parameterizedSql, baselineInfo);
            BaselineSyncController baselineSyncController = new BaselineSyncController();
            baselineSyncController.updateBaselineSync(schemaName, baselineInfo);
            result.addRow(new Object[] {baselineInfo.getId(), null, "HINT BIND :" + hint, null});
            return result;
        }
        String planJsonString = PlanManagerUtil.relNodeToJson(plan);
        if (!PlanManagerUtil.baselineSupported(plan)) {
            throw new TddlRuntimeException(ERR_BASELINE, "not support baseline add");
        }

        PlanInfo planInfo =
            planManager
                .createPlanInfo(schemaName, planJsonString, plan, baselineInfo.getId(), executionContext.getTraceId(),
                    PlanManagerUtil.getPlanOrigin(plan), ast, executionContext);
        planInfo.setFixed(fix);
        planInfo.setFixHint(hint);

        // Rebuild at load for plan with pushdown hint
        if (withPushdownHint) {
            baselineInfo.setRebuildAtLoad(true);
            // Skip post planner if pushdown hint used
            baselineInfo.setUsePostPlanner(executionPlan.isUsePostPlanner());
        }
        String planExplain = null;
        PlanInfo existedPlanInfo = baselineInfo.getAcceptedPlans().get(planInfo.getId());
        if (existedPlanInfo != null) {
            planExplain = explainCostPlan(executionPlan, executionContext);
            if (existedPlanInfo.isFixed() && fix) {
                result.addRow(
                    new Object[] {baselineInfo.getId(), planInfo.getId(), "fixed plan exist", planExplain});
                return result;
            } else if (existedPlanInfo.isFixed() && !fix) {
                result.addRow(
                    new Object[] {baselineInfo.getId(), planInfo.getId(), "fixed plan exist", planExplain});
                return result;
            } else if (!existedPlanInfo.isFixed() && !fix) {
                result.addRow(
                    new Object[] {baselineInfo.getId(), planInfo.getId(), "ExecutionPlan exists", planExplain});
                return result;
            } else { // !existedPlanInfo.isFixed() && fix
                // need to fix
            }
        }
        if (fix) {
            FeatureStats.getInstance().increment(FIX_PLAN_NUM);
        }
        baselineInfo.addAcceptedPlan(planInfo);
        baselineInfoMap.put(parameterizedSql, baselineInfo);
        BaselineSyncController baselineSyncController = new BaselineSyncController();
        baselineSyncController.updateBaselineSync(schemaName, baselineInfo);

        planExplain = explainCostPlan(executionPlan, executionContext);
        result.addRow(new Object[] {baselineInfo.getId(), planInfo.getId(), "OK", planExplain});
        return result;
    }

    protected Cursor baselineLPCVD(List<Long> idList, ExecutionContext executionContext, String operation,
                                   PlanManager planManager) {
        String schemaName = executionContext.getSchemaName();
        if (idList != null && !idList.isEmpty()) {
            ArrayResultCursor result = new ArrayResultCursor("baseline");
            result.addColumn("ID", DataTypes.IntegerType);
            result.addColumn("STATUS", DataTypes.StringType);
            for (Long id : idList) {
                switch (operation.toUpperCase()) {
                case "LOAD":
                    SyncManagerHelper.syncWithDefaultDB(new BaselineLoadSyncAction(), SyncScope.CURRENT_ONLY);
                    break;
                case "PERSIST":
                    SyncManagerHelper.syncWithDefaultDB(new BaselinePersistSyncAction(), SyncScope.CURRENT_ONLY);
                    break;
                case "DELETE": {
                    BaselineSyncController baselineSyncController = new BaselineSyncController();
                    boolean idFound = false;
                    for (Map.Entry<String, Map<String, BaselineInfo>> entry : planManager.getBaselineMap().entrySet()) {
                        String targetSchema = entry.getKey();
                        Map<String, BaselineInfo> map = entry.getValue();
                        for (BaselineInfo baselineInfo : map.values()) {
                            if (baselineInfo.getId() == id) {
                                idFound = true;
                                baselineSyncController.deleteBaseline(targetSchema, baselineInfo);
                            }
                        }
                    }
                    if (idFound) {
                        result.addRow(new Object[] {id, "OK"});
                    } else {
                        result.addRow(new Object[] {id, "not found"});
                    }
                    break;
                }
                case "DELETE_PLAN": {
                    BaselineSyncController baselineSyncController = new BaselineSyncController();
                    boolean idFound = false;
                    for (Map.Entry<String, Map<String, BaselineInfo>> entry : planManager.getBaselineMap().entrySet()) {
                        String targetSchema = entry.getKey();
                        Map<String, BaselineInfo> map = entry.getValue();
                        for (BaselineInfo baselineInfo : map.values()) {
                            for (PlanInfo planInfo : baselineInfo.getAcceptedPlans().values()) {
                                if (planInfo.getId() == id) {
                                    idFound = true;
                                    if (baselineInfo.getAcceptedPlans().size() == 1) {
                                        baselineSyncController.deleteBaseline(targetSchema, baselineInfo);
                                        break;
                                    } else {
                                        baselineSyncController.deletePlan(targetSchema, baselineInfo, planInfo);
                                    }
                                }
                            }
                            for (PlanInfo planInfo : baselineInfo.getUnacceptedPlans().values()) {
                                if (planInfo.getId() == id) {
                                    idFound = true;
                                    baselineSyncController.deletePlan(schemaName, baselineInfo, planInfo);
                                }
                            }
                        }
                    }
                    if (idFound) {
                        result.addRow(new Object[] {id, "OK"});
                    } else {
                        result.addRow(new Object[] {id, "not found"});
                    }
                    break;
                }
                }
            }
            return result;
        } else {
            switch (operation.toUpperCase()) {
            case "LOAD":
                SyncManagerHelper.syncWithDefaultDB(new BaselineLoadSyncAction(), SyncScope.CURRENT_ONLY);
                break;
            case "PERSIST":
                SyncManagerHelper.syncWithDefaultDB(new BaselinePersistSyncAction(), SyncScope.CURRENT_ONLY);
                break;
            case "DELETE_ALL": {
                BaselineSyncController baselineSyncController = new BaselineSyncController();
                for (BaselineInfo baselineInfo : planManager.getBaselineMap(schemaName).values()) {
                    baselineSyncController.deleteBaseline(schemaName, baselineInfo);
                }
                break;
            }
            case "DELETE_EVOLVED": {
                SyncManagerHelper.syncWithDefaultDB(new BaselineDeleteHotEvolvedSyncAction(schemaName),
                    SyncScope.CURRENT_ONLY);
                break;
            }
            default:
                throw new TddlRuntimeException(ERR_BASELINE,
                    String.format("not support baseline %s statement without baselineId", operation.toUpperCase()));
            }
        }
        ArrayResultCursor result = new ArrayResultCursor("baseline");
        result.addColumn("ID", DataTypes.IntegerType);
        result.addColumn("STATUS", DataTypes.StringType);
        if (idList != null && !idList.isEmpty()) {
            for (Long baselineId : idList) {
                result.addRow(new Object[] {baselineId, "OK"});
            }
        }
        return result;
    }

    private Cursor baselineList(List<Long> baselineIds, ExecutionContext executionContext, RelOptCluster cluster) {
        RelOptSchema relOptSchema =
            SqlConverter.getInstance(executionContext.getSchemaName(), executionContext).getCatalog();
        String schemaName = executionContext.getSchemaName();

        Set<Integer> baselineIdSet = new HashSet<>();
        if (baselineIds != null && !baselineIds.isEmpty()) {
            for (Long baselineId : baselineIds) {
                baselineIdSet.add(baselineId.intValue());
            }
        }

        ArrayResultCursor result = new ArrayResultCursor("baseline");
        result.addColumn("BASELINE_ID", DataTypes.IntegerType);
        result.addColumn("PARAMETERIZED_SQL", DataTypes.StringType);
        result.addColumn("PLAN_ID", DataTypes.IntegerType);
        result.addColumn("EXTERNALIZED_PLAN", DataTypes.StringType);
        result.addColumn("FIXED", DataTypes.TinyIntType);
        result.addColumn("ACCEPTED", DataTypes.TinyIntType);
        result.addColumn("ORIGIN", DataTypes.StringType);
        result.addColumn("IS_REBUILD_AT_LOAD", DataTypes.StringType);
        result.addColumn("HINT", DataTypes.StringType);
        result.addColumn("USE_POST_PLANNER", DataTypes.StringType);
        result.addColumn("HOT_EVOLVED", DataTypes.StringType);
        for (BaselineInfo baselineInfo : PlanManager.getInstance().getBaselineMap(schemaName).values()) {
            if (!baselineIdSet.isEmpty() && !baselineIdSet.contains(baselineInfo.getId())) {
                continue;
            }

            List<PlanInfo> displayList = new ArrayList<>(baselineInfo.getAcceptedPlans().values());
            displayList.addAll(baselineInfo.getUnacceptedPlans().values());

            // rebuild at load meaning this baseline only record hint instead of caching plans.
            if (baselineInfo.isRebuildAtLoad()) {
                Object[] row = new Object[11];
                row[0] = baselineInfo.getId();
                row[1] = baselineInfo.getParameterSql();
                row[2] = 0;
                row[3] = "";
                row[4] = 1;
                row[5] = 1;
                row[6] = "";
                row[7] = baselineInfo.isRebuildAtLoad() + "";
                row[8] = baselineInfo.getHint();
                row[9] = baselineInfo.isUsePostPlanner() + "";
                row[10] = String.valueOf(baselineInfo.isHotEvolution());
                result.addRow(row);
                continue;
            }

            for (PlanInfo planInfo : displayList) {
                String explainString;
                try {
                    explainString = RelUtils
                        .toString(PlanManagerUtil.jsonToRelNode(planInfo.getPlanJsonString(), cluster, relOptSchema));
                } catch (Throwable throwable) {
                    explainString = throwable.getMessage();
                }
                Object[] row = new Object[11];
                row[0] = baselineInfo.getId();
                row[1] = baselineInfo.getParameterSql();
                row[2] = planInfo.getId();
                row[3] = "\n" + explainString;
                row[4] = planInfo.isFixed();
                row[5] = planInfo.isAccepted();
                row[6] = planInfo.getOrigin();
                row[7] = baselineInfo.isRebuildAtLoad() + "";
                row[8] = planInfo.getFixHint();
                row[9] = baselineInfo.isUsePostPlanner() + "";
                row[10] = String.valueOf(baselineInfo.isHotEvolution());
                result.addRow(row);
            }
        }
        return result;
    }

    public String explainCostPlan(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        ExecutionContext ec = executionContext.copy();
        ExplainResult explainResult = new ExplainResult();
        explainResult.explainMode = ExplainResult.ExplainMode.COST;
        ec.setExplain(explainResult);
        ec.setCalcitePlanOptimizerTrace(new CalcitePlanOptimizerTrace());
        ResultCursor rc = ExplainExecutorUtil.explain(executionPlan, ec, explainResult);
        StringBuilder explainString = new StringBuilder("\n");
        Row row;
        while ((row = rc.next()) != null) {
            explainString.append(row.getString(0)).append("\n");
        }
        return explainString.toString();
    }
}

