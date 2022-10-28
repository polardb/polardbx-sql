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
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.PlannerContextWithParam;

/**
 * Created by chuanqin on 18/1/3.
 */
public class CalcitePlanOptimizerTrace {
    public static SqlExplainLevel DEFAULT_LEVEL = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
    private boolean open;
    private SqlExplainLevel sqlExplainLevel;
    private PlanOptimizerTracer optimizerTracer;
    public CalcitePlanOptimizerTrace() {
        this.open = false;
        this.sqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
        this.optimizerTracer = new PlanOptimizerTracer();
    }

    public PlanOptimizerTracer getOptimizerTracer() {
        return optimizerTracer;
    }


    public boolean isOpen() {
        return open;
    }

    public void setOpen(boolean b) {
        this.open = b;
    }

    public SqlExplainLevel getSqlExplainLevel() {
        return sqlExplainLevel;
    }
    
    public void setSqlExplainLevel(SqlExplainLevel v) {
        this.sqlExplainLevel = v;
    }

    public void savePlanDisplayIfNecessary(RelNode rootPlan, Context context) {
        if (open) {
            optimizerTracer.savePlanDisplayIfNecessary(rootPlan, context, getSqlExplainLevel());
        }
    }

    public void traceIt(RelOptRuleCall ruleCall) {
        if (open) {
            optimizerTracer.traceIt(ruleCall);
        }
    }


    public void addSnapshot(String ruleName, RelNode plan, PlannerContextWithParam context) {
        if (open) {
            optimizerTracer.addSnapshot(ruleName, plan, context, getSqlExplainLevel());
        }
    }

    public static void savePlanDisplayIfNecessaryFromContext(RelNode rootPlan, Context context) {
        if (context instanceof PlannerContextWithParam) {
            ((PlannerContextWithParam) context).getCalcitePlanOptimizerTrace().ifPresent(x ->
                x.savePlanDisplayIfNecessary(rootPlan, context));
        }
    }

    public static void traceItFromContext(RelOptRuleCall ruleCall, Context context) {
        if (context instanceof PlannerContextWithParam) {
            ((PlannerContextWithParam) context).getCalcitePlanOptimizerTrace().ifPresent(x ->
                x.traceIt(ruleCall));
        }
    }
}
