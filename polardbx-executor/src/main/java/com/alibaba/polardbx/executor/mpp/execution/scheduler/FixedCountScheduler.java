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

package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.executor.mpp.execution.RemoteTask;
import com.alibaba.polardbx.executor.mpp.execution.SqlStageExecution;
import com.alibaba.polardbx.executor.mpp.util.ImmutableCollectors;
import com.alibaba.polardbx.gms.node.Node;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

public class FixedCountScheduler implements StageScheduler {

    private final Map<Integer, Node> partitionToNode;
    private final BiFunction<Node, Integer, RemoteTask> taskScheduler;
    private final int driverParallelism;

    public FixedCountScheduler(SqlStageExecution stage, Map<Integer, Node> partitionToNode) {
        requireNonNull(stage, "stage is null");
        int totalParallelism = stage.getFragment().getPartitioning().getPartitionCount();
        this.taskScheduler = stage::scheduleTask;
        this.partitionToNode = requireNonNull(partitionToNode, "partitionToNode is null");
        int taskNum = partitionToNode.size();
        this.driverParallelism = totalParallelism % taskNum > 0 ? totalParallelism / taskNum + 1 :
            totalParallelism / taskNum;
    }

    @Override
    public ScheduleResult schedule() {
        List<RemoteTask> newTasks = partitionToNode.entrySet().stream()
            .map(entry -> taskScheduler.apply(entry.getValue(), entry.getKey()))
            .collect(ImmutableCollectors.toImmutableList());
        return new ScheduleResult(true, newTasks, 0);
    }

    @Override
    public int getTaskNum() {
        return partitionToNode.size();
    }

    @Override
    public int requireChildOutputNum() {
        return getTaskNum();
    }

    @Override
    public int getDriverParallelism() {
        return driverParallelism;
    }
}
