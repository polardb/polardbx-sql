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

package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner;
import com.alibaba.polardbx.executor.mpp.operator.factory.LocalBufferExecutorFactory;
import com.alibaba.polardbx.executor.mpp.operator.factory.PipelineFactory;
import com.alibaba.polardbx.executor.mpp.split.SplitManagerImpl;
import com.alibaba.polardbx.executor.mpp.util.MoreExecutors;
import com.alibaba.polardbx.executor.operator.spill.MemorySpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class PlanUtils {

    public static String textLocalPlan(ExecutionContext context, RelNode relNode, ExecutorMode type) {
        if (context.getMemoryPool() == null) {
            context.setMemoryPool(MemoryManager.getInstance().createQueryMemoryPool(
                WorkloadUtil.isApWorkload(
                    context.getWorkloadType()), context.getTraceId(), context.getExtraCmds()));
        }

        if (context.getExecuteMode() == ExecutorMode.TP_LOCAL && context.getParamManager().getInt(
            ConnectionParams.PARALLELISM) == -1) {
            context.getExtraCmds().put(ConnectionProperties.PARALLELISM, 1);
        }

        int parallelism = ExecUtils.getParallelismForLocal(context);

        StringBuilder builder = new StringBuilder();
        builder.append("ExecutorMode: ").append(type).append(" ").append("\n");
        outputLocalFragment(context, parallelism, relNode, builder);
        return builder.toString();
    }

    private static void outputLocalFragment(ExecutionContext context, int parallelism, RelNode relNode,
                                            StringBuilder builder) {
        boolean isSpill =
            MemorySetting.ENABLE_SPILL && context.getParamManager().getBoolean(ConnectionParams.ENABLE_SPILL);
        LocalExecutionPlanner planner =
            new LocalExecutionPlanner(context, null, parallelism, parallelism, 1,
                context.getParamManager().getInt(ConnectionParams.PREFETCH_SHARDS), MoreExecutors.directExecutor(),
                isSpill ? new MemorySpillerFactory() : null, null, null, false,
                -1, -1, ImmutableMap.of(), new SplitManagerImpl());
        List<DataType> columns = CalciteUtils.getTypes(relNode.getRowType());
        OutputBufferMemoryManager localBufferManager = planner.createLocalMemoryManager();
        LocalBufferExecutorFactory factory = new LocalBufferExecutorFactory(localBufferManager, columns, 1);
        List<PipelineFactory> pipelineFactories = planner.plan(relNode, factory, localBufferManager,
            context.getTraceId());

        for (PipelineFactory pipelineFactory : pipelineFactories) {
            builder.append(formatPipelineFragment(context, pipelineFactory, context.getParams().getCurrentParameter()));
        }
    }

    public static String formatPipelineFragment(ExecutionContext executionContext,
                                                PipelineFactory pipelineFactory,
                                                Map<Integer, ParameterContext> params) {
        PipelineFragment fragment = pipelineFactory.getFragment();
        StringBuilder builder = new StringBuilder();
        builder.append(format("Pipeline %s dependency: [%s] parallelism: %s", fragment.getPipelineId(),
            Joiner.on(", ").join(fragment.getDependency()), fragment.getParallelism()));
        if (fragment.getPrefetchLists().size() > 0) {
            builder.append(format(" prefetch: %s \n ", fragment.getPrefetchLists()));
        } else {
            builder.append(" \n ");
        }
        builder.append(
                RelUtils.toString(executionContext.getSqlExplainLevel(), fragment.getProperties().getRelNode(), params))
            .append("\n");
        return builder.toString();
    }

    public static String textPlan(ExecutionContext executionContext, Session session, RelNode relNode) {
        Pair<SubPlan, Integer> plan = PlanFragmenter.buildRootFragment(relNode, session);
        StringBuilder builder = new StringBuilder();
        builder.append("ExecutorType: ").append("MPP").append("\n");
        builder.append("The Query's MaxConcurrentParallelism: ").append(plan.getValue()).append("\n");
        for (PlanFragment fragment : plan.getKey().getAllFragments()) {
            builder.append(formatFragment(
                executionContext, fragment, session.getClientContext().getParams().getCurrentParameter()));
            if (executionContext.getParamManager().getBoolean(ConnectionParams.SHOW_PIPELINE_INFO_UNDER_MPP)) {
                int mppNodeSize = executionContext.getParamManager().getInt(ConnectionParams.MPP_NODE_SIZE);
                if (mppNodeSize <= 0) {
                    mppNodeSize = ExecUtils.getActiveNodeCount();
                }
                int parallelism = fragment.getPartitioning().getPartitionCount() / mppNodeSize;
                outputLocalFragment(executionContext, Math.max(1, parallelism), fragment.getRootNode(), builder);
            }
        }
        return builder.toString();
    }

    private static String formatFragment(ExecutionContext executionContext, PlanFragment fragment,
                                         Map<Integer, ParameterContext> params) {
        StringBuilder builder = new StringBuilder();
        builder.append(format("Fragment %s \n", fragment.getId()));

        PartitioningScheme partitioningScheme = fragment.getPartitioningScheme();
        builder.append(indentString(1));
        builder.append(format("Output partitioning: %s [%s] ",
            partitioningScheme.getPartitionMode(), Joiner.on(", ").join(partitioningScheme.getPartChannels())));
        builder.append(format("Parallelism: %s ",
            fragment.getPartitioning().getPartitionCount()));
        if (fragment.getAllSplitNums() > 0) {
            builder.append(format("Splits: %s \n", fragment.getAllSplitNums()));
        } else {
            builder.append("\n");
        }

        builder.append(indentString(1));
        builder.append(RelUtils.toString(executionContext.getSqlExplainLevel(), fragment.getRootNode(), params))
            .append("\n");
        return builder.toString();
    }

    private static String indentString(int indent) {
        return Strings.repeat("    ", indent);
    }

}