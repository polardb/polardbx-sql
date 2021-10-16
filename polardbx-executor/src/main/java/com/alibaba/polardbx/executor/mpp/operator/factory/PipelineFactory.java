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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.executor.mpp.operator.DriverContext;
import com.alibaba.polardbx.executor.mpp.operator.DriverExec;
import com.alibaba.polardbx.executor.mpp.planner.PipelineFragment;
import com.alibaba.polardbx.executor.mpp.planner.PipelineProperties;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.operator.ConsumerExecutor;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;

import java.util.List;
import java.util.Set;

public class PipelineFactory {

    protected final ExecutorFactory produceFactory;
    protected final ConsumeExecutorFactory consumeFactory;
    protected PipelineFragment fragment;

    public PipelineFactory(ExecutorFactory produceFactory, ConsumeExecutorFactory consumeFactory,
                           PipelineFragment fragment) {
        this.produceFactory = produceFactory;
        this.consumeFactory = consumeFactory;
        this.fragment = fragment;
    }

    public DriverExec createDriverExec(ExecutionContext context, DriverContext driverContext, int index) {
        Executor producer = produceFactory.createExecutor(context, index);
        ConsumerExecutor consumerExecutor = consumeFactory.createExecutor(context, index);
        int pipelineId = fragment.getPipelineId();
        return new DriverExec(pipelineId, driverContext, producer, consumerExecutor, fragment.getParallelism());
    }

    public PipelineFragment getFragment() {
        return fragment;
    }

    public List<LogicalView> getLogicalView() {
        return fragment.getLogicalView();
    }

    public SplitInfo getSource(Integer id) {
        return fragment.getSource(id);
    }

    public Set<Integer> getDependency() {
        return fragment.getDependency();
    }

    public int getPipelineId() {
        return fragment.getPipelineId();
    }

    public int getParallelism() {
        return fragment.getParallelism();
    }

    public ExecutorFactory getProduceFactory() {
        return produceFactory;
    }

    public ConsumeExecutorFactory getConsumeFactory() {
        return consumeFactory;
    }

    public PipelineProperties getProperties() {
        return fragment.getProperties();
    }
}
