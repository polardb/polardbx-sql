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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.execution.RemoteTask;
import com.alibaba.polardbx.executor.mpp.execution.SqlStageExecution;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SourcePartitionedScheduler implements StageScheduler {

    private static final Logger log = LoggerFactory.getLogger(SourcePartitionedScheduler.class);

    private final SqlStageExecution stage;

    private Map<Node, Multimap<Integer, Split>> splitAssignments;

    private final int driverParallelism;

    public SourcePartitionedScheduler(
        SqlStageExecution stageExecution, SplitPlacementPolicy splitPlacementPolicy,
        SplitInfo splitInfo, List<SplitInfo> expandSplitInfos, ExecutionContext context) {
        this.stage = stageExecution;
        this.splitAssignments = new HashMap<>();
        List<Split> splits = Iterables.getOnlyElement(splitInfo.getSplits());
        Multimap<Node, Split> splitAssignment = splitPlacementPolicy.computeAssignments(splits);

        for (Map.Entry<Node, Split> entry : splitAssignment.entries()) {
            if (!splitAssignments.containsKey(entry.getKey())) {
                Multimap<Integer, Split> multimap = HashMultimap.create();
                splitAssignments.put(entry.getKey(), multimap);
            }
            splitAssignments.get(entry.getKey()).put(splitInfo.getSourceId(), entry.getValue());
        }

        if (expandSplitInfos != null && expandSplitInfos.size() > 0) {
            for (SplitInfo info : expandSplitInfos) {
                List<Split> expandSplits = Iterables.getOnlyElement(info.getSplits());
                for (Map.Entry<Node, Split> entry : splitAssignment.entries()) {
                    splitAssignments.get(entry.getKey()).putAll(info.getSourceId(), expandSplits);
                }
            }
        }

        int totalParallelism = stageExecution.getFragment().getPartitioning().getPartitionCount();
        int taskNum = splitAssignments.keySet().size();
        if (splitInfo.isUnderSort()) {
            this.driverParallelism = 1;
        } else {
            this.driverParallelism = totalParallelism % taskNum > 0 ? totalParallelism / taskNum + 1 :
                totalParallelism / taskNum;
        }

        int totalPrefetch = 0;
        if (splitInfo.getConcurrencyPolicy() == QueryConcurrencyPolicy.SEQUENTIAL) {
            totalPrefetch = 1;
        } else {
            context.getParamManager().getInt(ConnectionParams.PREFETCH_SHARDS);
            if (totalPrefetch < 0) {
                totalPrefetch = ExecUtils.getMppPrefetchNumForLogicalView(splitInfo.getDbCount());
                if (expandSplitInfos != null && expandSplitInfos.size() > 0) {
                    for (SplitInfo info : expandSplitInfos) {
                        totalPrefetch = Math.max(
                            totalPrefetch, ExecUtils.getMppPrefetchNumForLogicalView(info.getDbCount()));
                    }
                }
            }
        }

        totalPrefetch = totalPrefetch > totalParallelism ? totalPrefetch : totalParallelism;

        int prefetch = totalPrefetch % taskNum > 0 ? totalPrefetch / taskNum + 1 :
            totalPrefetch / taskNum;

        log.debug("sourceId: " + splitInfo.getSourceId() + "splits num: " + splitInfo.getSplitCount() + ", prefetch: "
            + prefetch);

        int bkaJoinParallelism = stage.getFragment().getBkaJoinParallelism() % taskNum > 0 ?
            stage.getFragment().getBkaJoinParallelism() / taskNum + 1 :
            stage.getFragment().getBkaJoinParallelism() / taskNum;

        stageExecution.getFragment().setPrefetch(prefetch);
        stageExecution.getFragment().setBkaJoinParallelism(bkaJoinParallelism);
    }

    @Override
    public ScheduleResult schedule() {

        List<RemoteTask> newTasks = new ArrayList<>();
        for (Map.Entry<Node, Multimap<Integer, Split>> taskSplits : splitAssignments.entrySet()) {
            newTasks.addAll(stage.scheduleSplits(
                taskSplits.getKey(), taskSplits.getValue(), true));
        }
        return new ScheduleResult(true, newTasks, 0);
    }

    @Override
    public int getTaskNum() {
        return splitAssignments.keySet().size();
    }

    @Override
    public int getDriverParallelism() {
        return driverParallelism;
    }
}
