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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlaceHolderExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.parametric.BaseParametricQueryAdvisor;
import com.alibaba.polardbx.optimizer.planmanager.parametric.Point;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.type.SqlTypeName;

import java.text.NumberFormat;
import java.util.Map;
import java.util.Set;

/**
 * @author fangwu
 */
public class FetchSPMSyncAction implements ISyncAction {

    private String schemaName = null;

    private boolean withPlan;

    public FetchSPMSyncAction() {
    }

    public FetchSPMSyncAction(String schemaName) {
        this.schemaName = schemaName;
        this.withPlan = true;
    }

    public FetchSPMSyncAction(String schemaName, boolean withPlan) {
        this.schemaName = schemaName;
        this.withPlan = withPlan;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public boolean isWithPlan() {
        return withPlan;
    }

    public void setWithPlan(boolean withPlan) {
        this.withPlan = withPlan;
    }

    @Override
    public ResultCursor sync() {
        PlanManager planManager = PlanManager.getInstance();
        Map<String, BaselineInfo> baselineMap = planManager.getBaselineMap(schemaName);

        ArrayResultCursor result = new ArrayResultCursor("PLAN_CACHE");
        result.addColumn("BASELINE_ID", DataTypes.StringType);
        result.addColumn("SCHEMA_NAME", DataTypes.StringType);
        result.addColumn("PLAN_ID", DataTypes.LongType);
        result.addColumn("FIXED", DataTypes.BooleanType);
        result.addColumn("ACCEPTED", DataTypes.BooleanType);
        result.addColumn("CHOOSE_COUNT", DataTypes.LongType);
        result.addColumn("SELECTIVITY_SPACE", DataTypes.StringType);
        result.addColumn("PARAMS", DataTypes.StringType);
        result.addColumn("RECENTLY_CHOOSE_RATE", DataTypes.StringType);
        result.addColumn("EXPECTED_ROWS", DataTypes.LongType);
        result.addColumn("MAX_ROWS_FEEDBACK", DataTypes.LongType);
        result.addColumn("MIN_ROWS_FEEDBACK", DataTypes.LongType);
        result.addColumn("ORIGIN", DataTypes.StringType);
        result.addColumn("PARAMETERIZED_SQL", DataTypes.StringType);
        result.addColumn("EXTERNALIZED_PLAN", DataTypes.StringType);
        result.addColumn("IS_REBUILD_AT_LOAD", DataTypes.StringType);
        result.addColumn("HINT", DataTypes.StringType);
        result.addColumn("USE_POST_PLANNER", DataTypes.StringType);

        if (baselineMap == null) {
            return result;
        }

        for (Map.Entry<String, BaselineInfo> entry : baselineMap.entrySet()) {
            String paramSql = entry.getKey();
            BaselineInfo baselineInfo = entry.getValue();
            Set<Point> points = baselineInfo.getPointSet();
            if (baselineInfo.isRebuildAtLoad()) {
                result.addRow(new Object[] {
                    baselineInfo.getId(),
                    schemaName,
                    0, // plan id
                    1, // isFixed
                    1, // isAccepted
                    0, // chooseCount
                    null, // point info
                    null, // point info
                    null, // point info
                    null, // point info
                    null, // point info
                    null, // point info
                    null, // workload type
                    paramSql,
                    "", // plan
                    baselineInfo.isRebuildAtLoad() + "",
                    baselineInfo.getHint(),
                    baselineInfo.isUsePostPlanner() + ""
                });
            }
            for (PlanInfo planInfo : baselineInfo.getPlans()) {
                RelNode plan = planInfo.getPlan(null, null);
                String planExplain = null;
                if (plan != null) {
                    planExplain = "\n" + RelOptUtil.dumpPlan("",
                        plan,
                        SqlExplainFormat.TEXT,
                        SqlExplainLevel.NO_ATTRIBUTES);
                }
                Point point = findPoint(points, planInfo.getId());
                NumberFormat numberFormat = NumberFormat.getPercentInstance();
                result.addRow(new Object[] {
                    baselineInfo.getId(),
                    schemaName,
                    planInfo.getId(),
                    planInfo.isFixed(),
                    planInfo.isAccepted(),
                    planInfo.getChooseCount(),
                    point == null ? null : point.getSelectivityMap().toString(),
                    point == null ? null : point.getParams().toString(),
                    point == null ? null : numberFormat.format(point.getLastRecentlyChooseRate()) + "",
                    point == null ? null : point.getRowcountExpected() + "",
                    point == null ? null : point.getMaxRowcountExpected() + "",
                    point == null ? null : point.getMinRowcountExpected() + "",
                    planInfo.getOrigin(),
                    paramSql,
                    planExplain,
                    baselineInfo.isRebuildAtLoad() + "",
                    baselineInfo.getHint(),
                    baselineInfo.isUsePostPlanner() + ""
                });
            }
        }

        return result;
    }

    private Point findPoint(Set<Point> points, int planId) {
        if (points == null) {
            return null;
        }
        for (Point point : points) {
            if (point != null && point.getPlanId() == planId) {
                return point;
            }
        }
        return null;
    }
}

