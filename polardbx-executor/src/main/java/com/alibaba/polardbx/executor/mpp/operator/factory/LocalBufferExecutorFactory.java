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

import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.buffer.OutputBufferMemoryManager;
import com.alibaba.polardbx.executor.mpp.operator.LocalAllBufferExec;
import com.alibaba.polardbx.executor.mpp.operator.LocalBufferExec;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.ArrayList;
import java.util.List;

public class LocalBufferExecutorFactory extends ExecutorFactory {

    private List<Executor> executors = new ArrayList<>();
    private int parallelism;

    private OutputBufferMemoryManager outputBufferMemoryManager;
    private List<DataType> columnMetaList;
    // 本地模式时给最终结果SmpResultCursor使用
    private boolean syncMode;

    private boolean spillOutput;

    public LocalBufferExecutorFactory(OutputBufferMemoryManager outputBufferMemoryManager,
                                      List<DataType> columnMetaList,
                                      int parallelism) {
        this(outputBufferMemoryManager, columnMetaList, parallelism, false);
    }

    public LocalBufferExecutorFactory(OutputBufferMemoryManager outputBufferMemoryManager,
                                      List<DataType> columnMetaList,
                                      int parallelism,
                                      boolean syncMode) {
        this(outputBufferMemoryManager, columnMetaList, parallelism, syncMode, false);
    }

    public LocalBufferExecutorFactory(OutputBufferMemoryManager outputBufferMemoryManager,
                                      List<DataType> columnMetaList,
                                      int parallelism,
                                      boolean syncMode,
                                      boolean spillOutput) {
        this.parallelism = parallelism;
        this.columnMetaList = columnMetaList;
        this.outputBufferMemoryManager = outputBufferMemoryManager;
        this.syncMode = syncMode;
        this.spillOutput = spillOutput;
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        createAllExecutor(context);
        return executors.get(index);
    }

    @Override
    public List<Executor> getAllExecutors(ExecutionContext context) {
        return createAllExecutor(context);
    }

    private synchronized List<Executor> createAllExecutor(ExecutionContext context) {
        if (executors.isEmpty()) {
            for (int i = 0; i < parallelism; i++) {
                LocalBufferExec bufferExec;
                if (spillOutput) {
                    bufferExec = new LocalAllBufferExec(context,
                        outputBufferMemoryManager, columnMetaList,
                        ServiceProvider.getInstance().getServer().getSpillerFactory());
                } else {
                    bufferExec = new LocalBufferExec(outputBufferMemoryManager, columnMetaList, syncMode);
                }
                executors.add(bufferExec);
            }
        }
        return executors;
    }
}