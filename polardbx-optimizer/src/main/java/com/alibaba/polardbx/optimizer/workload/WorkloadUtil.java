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

package com.alibaba.polardbx.optimizer.workload;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.RemoveFixedCostVisitor;
import com.alibaba.polardbx.optimizer.planmanager.LogicalViewFinder;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;

/**
 * @author dylan
 */
public class WorkloadUtil {

    public static WorkloadType determineWorkloadType(RelNode rel, RelMetadataQuery mq) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(rel);
        if (!plannerContext.getSqlKind().belongsTo(SqlKind.QUERY)) {
            return WorkloadType.TP;
        }
        rel.accept(new RemoveFixedCostVisitor());
        LogicalViewFinder logicalViewFinder = new LogicalViewFinder();
        rel.accept(logicalViewFinder);
        double ossNetThreshold = plannerContext.getParamManager().getLong(ConnectionParams.WORKLOAD_OSS_NET_THRESHOLD);
        for (LogicalView logicalView : logicalViewFinder.getResult()) {
            if (logicalView instanceof OSSTableScan) {
                RelOptCost ossTableScanCost = mq.getCumulativeCost(logicalView);
                if (ossTableScanCost.getNet() > ossNetThreshold) {
                    return WorkloadType.AP;
                }
            }
        }
        RelOptCost cost = mq.getCumulativeCost(rel);
        double ioThreshold = plannerContext.getParamManager().getLong(ConnectionParams.WORKLOAD_IO_THRESHOLD);
        return cost.getIo() < ioThreshold ? WorkloadType.TP : WorkloadType.AP;
    }

    public static WorkloadType getWorkloadType(ExecutionContext executionContext) {
        return getWorkloadType(executionContext, executionContext.getFinalPlan());
    }

    public static WorkloadType getWorkloadType(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        RelNode plan;
        if (executionPlan == null) {
            plan = null;
        } else {
            plan = executionPlan.getPlan();
        }
        return getWorkloadType(executionContext, plan);
    }

    public static WorkloadType getWorkloadType(ExecutionContext executionContext, RelNode plan) {
        if (executionContext.getWorkloadType() != null) {
            return executionContext.getWorkloadType();
        }
        WorkloadType targetWorkType;
        if (plan == null) {
            targetWorkType = WorkloadType.TP;
        } else {
            targetWorkType = PlannerContext.getPlannerContext(plan).getWorkloadType();
        }
        if (targetWorkType == null) {
            targetWorkType = WorkloadType.TP;
        }
        executionContext.setWorkloadType(targetWorkType);
        return targetWorkType;
    }

    public static boolean isApWorkload(WorkloadType workloadType) {
        return workloadType == WorkloadType.AP;
    }
}
