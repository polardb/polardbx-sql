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

package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;

import java.util.Map;

/**
 * @author roy
 */
public interface PlanManageable extends Lifecycle {
    /**
     * 根据参数化 sql 和 context 信息构造出执行计划
     */
    ExecutionPlan choosePlan(SqlParameterized sqlParameterized, ExecutionContext executionContext);

    /**
     * 反馈执行计划执行情况到 Plan Manager
     */
    void feedBack(ExecutionPlan executionPlan, Throwable ex,
                  Map<RelNode, RuntimeStatisticsSketch> runtimeStatistics, ExecutionContext executionContext);

    /**
     * 演化执行计划的执行情况
     */
    void doEvolution(BaselineInfo baselineInfo, PlanInfo planInfo, long lastExecuteUnixTime,
                     double executionTimeInSeconds, Throwable ex);
}
