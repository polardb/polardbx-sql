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

package org.apache.calcite.util.trace;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.PlannerContextWithParam;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by chuanqin on 18/1/3.
 */
public class PlanOptimizerTracer {

    private String rootPlanForDisplay;
    private List<Map.Entry<String, String>> optimizedPlanSnapshots = new ArrayList<>();
    private Map<RelNode, RuntimeStatisticsSketch> runtimeStatistics;

    private class SnapShotEntry implements Map.Entry<String, String> {

        private final String key;
        private String value;

        public SnapShotEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String setValue(String value) {
            String old = this.value;
            this.value = value;
            return old;
        }

    }

    public void savePlanDisplayIfNecessary(RelNode rootPlan, Context context, SqlExplainLevel sqlExplainLevel) {
        RelDrdsWriter relWriter = new RelDrdsWriter(sqlExplainLevel);
        if (context instanceof PlannerContextWithParam) {
            final PlannerContextWithParam ctxWithParam = (PlannerContextWithParam) context;
            relWriter =
                new RelDrdsWriter(null, sqlExplainLevel, ctxWithParam.getParams().getCurrentParameter(), ctxWithParam.getEvalFunc(),
                    ctxWithParam.getExecContext());
        }
        rootPlan.explainForDisplay(relWriter);
        rootPlanForDisplay = relWriter.asString();
        relWriter.done(rootPlan);
    }

    public void traceIt(RelOptRuleCall ruleCall) {
        addRootPlan(ruleCall.getRule().toString());
    }

    public void addSnapshot(String ruleName, String plan) {
        SnapShotEntry snapShotEntry = new SnapShotEntry(ruleName, plan);
        optimizedPlanSnapshots.add(snapShotEntry);
    }

    public void addSnapshot(String ruleName, RelNode plan, PlannerContextWithParam context, SqlExplainLevel sqlExplainLevel) {
        RelDrdsWriter relWriter = new RelDrdsWriter(sqlExplainLevel);
        if (null != context) {
            relWriter =
                new RelDrdsWriter(null, sqlExplainLevel, context.getParams().getCurrentParameter(), context.getEvalFunc(),
                    context.getExecContext());
        }
        plan.explainForDisplay(relWriter);
        String logicalPlanString = relWriter.asString();
        relWriter.done(plan);
        SnapShotEntry snapShotEntry = new SnapShotEntry(ruleName, logicalPlanString);
        optimizedPlanSnapshots.add(snapShotEntry);
    }

    private void addRootPlan(String ruleName) {
        addSnapshot(ruleName, rootPlanForDisplay);
    }

    public List<Map.Entry<String, String>> getPlanSnapshots() {
        return optimizedPlanSnapshots;
    }

    public void clean() {
        optimizedPlanSnapshots.clear();
    }

    public Map<RelNode, RuntimeStatisticsSketch> getRuntimeStatistics() {
        return runtimeStatistics;
    }

    public void setRuntimeStatistics(Map<RelNode, RuntimeStatisticsSketch> runtimeStatistics) {
        this.runtimeStatistics = runtimeStatistics;
    }
}
