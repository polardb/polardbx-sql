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

package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBuffer;
import com.alibaba.polardbx.executor.mpp.operator.ExchangeClientSupplier;
import com.alibaba.polardbx.executor.mpp.operator.LocalExecutionPlanner;
import com.alibaba.polardbx.executor.mpp.planner.PlanFragment;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import io.airlift.http.client.HttpClient;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;

import static java.util.Objects.requireNonNull;

public class SqlTaskExecutionFactory {
    private final Executor taskNotificationExecutor;

    private final TaskExecutor taskExecutor;

    private final ExchangeClientSupplier exchangeClientSupplier;

    private final SpillerFactory spillerFactory;

    private final HttpClient httpClient;

    public SqlTaskExecutionFactory(
        Executor taskNotificationExecutor,
        TaskExecutor taskExecutor,
        ExchangeClientSupplier exchangeClientSupplier,
        SpillerFactory spillerFactory,
        HttpClient httpClient) {
        this.taskNotificationExecutor = requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
        this.exchangeClientSupplier = exchangeClientSupplier;
        this.spillerFactory = spillerFactory;
        this.httpClient = httpClient;
    }

    public SqlTaskExecution create(Session session, QueryContext queryContext, TaskStateMachine taskStateMachine,
                                   OutputBuffer outputBuffer, PlanFragment fragment, List<TaskSource> sources,
                                   URI uri) {
        TaskContext taskContext = queryContext.createTaskContext(
            taskStateMachine,
            session);
        int driverParallelism = fragment.getDriverParallelism();
        LocalExecutionPlanner planner = new LocalExecutionPlanner(
            session.getClientContext(),
            exchangeClientSupplier,
            driverParallelism,
            fragment.getBkaJoinParallelism(),
            fragment.getPartitioning().getPartitionCount(),
            fragment.getPrefetch(),
            taskNotificationExecutor,
            taskContext.isSpillable() ? spillerFactory : null,
            httpClient, uri, true);

        return SqlTaskExecution.createSqlTaskExecution(
            taskStateMachine,
            taskContext,
            outputBuffer,
            fragment,
            session,
            planner,
            sources,
            taskExecutor,
            taskNotificationExecutor,
            planner.getBloomFilterExpressionMap());
    }
}
