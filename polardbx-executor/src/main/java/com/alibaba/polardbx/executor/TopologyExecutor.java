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

package com.alibaba.polardbx.executor;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.exception.ExecutorException;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import org.apache.calcite.rel.RelNode;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class TopologyExecutor extends AbstractLifecycle implements ITopologyExecutor {

    protected String dataNode = "localhost";

    protected ServerThreadPool executorService;

    protected TopologyHandler handler;

    @Override
    public Cursor execByExecPlanNode(RelNode relNode, ExecutionContext executionContext) {
        String targetGroup = ExecUtils.getTargetDbGruop(relNode, executionContext);
        return getGroupExecutor(targetGroup).execByExecPlanNode(relNode, executionContext);
    }

    @Override
    public Future<Cursor> execByExecPlanNodeFuture(RelNode relNode, ExecutionContext executionContext) {
        return execByExecPlanNodeFuture(relNode, executionContext, null);
    }

    @Override
    public Future<Cursor> execByExecPlanNodeFuture(RelNode relNode, ExecutionContext executionContext,
                                                   BlockingQueue completionQueue) {
        final Map mdcContext = MDC.getCopyOfContextMap();
        ServerThreadPool concurrentExecutors = executionContext.getExecutorService();
        if (concurrentExecutors == null) {
            throw new ExecutorException("concurrentExecutors is null, cannot query parallelly");
        }
        OptimizerContext optimizerContext = OptimizerContext.getContext(executionContext.getSchemaName());

        Callable callTask = () -> {
            try {
                long startExecNano = System.nanoTime();
                long threadCpuTime = ThreadCpuStatUtil.getThreadCpuTimeNano();
                OptimizerContext.setContext(optimizerContext);
                MDC.setContextMap(mdcContext);
                Cursor cursor = execByExecPlanNode(relNode, executionContext);
                RuntimeStatHelper.registerAsyncTaskCpuStatForCursor(cursor,
                    ThreadCpuStatUtil.getThreadCpuTimeNano() - threadCpuTime,
                    System.nanoTime() - startExecNano);
                return cursor;
            } finally {
                // 映射设置了callerRun模式,不能直接清理MDC,否则会清掉自己的
                // ThreadLocalMap.reset();
            }
        };

        Future<Cursor> task = concurrentExecutors.submit(executionContext.getSchemaName(),
            executionContext.getTraceId(), -1,
            callTask,
            completionQueue,
            executionContext.getRuntimeStatistics());
        return task;
    }

    @Override
    public String getDataNode() {
        return dataNode;
    }

    @Override
    public IGroupExecutor getGroupExecutor(String group) {
        IExecutor executor = handler.get(group);
        if (executor == null) {
            throw new ExecutorException("Cannot find executor for group:" + group + "\ngroups:\n" + handler);
        }

        return (IGroupExecutor) executor;
    }

    @Override
    public void setExecutorService(ServerThreadPool executorService) {
        this.executorService = executorService;
    }

    @Override
    public ServerThreadPool getExecutorService() {
        return executorService;
    }

    @Override
    public void setTopology(TopologyHandler handler) {
        this.handler = handler;
    }

    @Override
    public TopologyHandler getTopology() {
        return handler;
    }

}
