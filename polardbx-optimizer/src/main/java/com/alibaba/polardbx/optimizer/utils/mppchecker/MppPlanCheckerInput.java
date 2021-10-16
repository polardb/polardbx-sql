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

package com.alibaba.polardbx.optimizer.utils.mppchecker;

import com.alibaba.polardbx.optimizer.PlannerContext;
import org.apache.calcite.rel.RelNode;

public class MppPlanCheckerInput {
    private final RelNode originalPlan;
    private final PlannerContext plannerContext;

    public MppPlanCheckerInput(RelNode originalPlan, PlannerContext plannerContext) {
        this.originalPlan = originalPlan;
        this.plannerContext = plannerContext;
    }

    public RelNode getOriginalPlan() {
        return originalPlan;
    }

    public PlannerContext getPlannerContext() {
        return plannerContext;
    }
}
