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

import com.alibaba.polardbx.executor.operator.CacheExec;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.ArrayList;
import java.util.List;

public class CacheExecFactory extends ExecutorFactory {

    private int parallelism;
    private List<Executor> executors = new ArrayList<>();
    private List<DataType> dataTypes;

    public CacheExecFactory(List<DataType> dataTypes, int parallelism) {
        this.parallelism = parallelism;
        this.dataTypes = dataTypes;
    }

    public CacheExecFactory(int parallelism, List<Executor> executors) {
        this.parallelism = parallelism;
        this.executors = executors;
        this.dataTypes = executors.get(0).getDataTypes();
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        createAllExecutors(context);
        CacheExec cacheExec = (CacheExec) executors.get(index);
        if (cacheExec.isReady()) {
            return new CacheExec(cacheExec.getDataTypes(), cacheExec.getChunks(), context);
        }
        return cacheExec;
    }

    @Override
    public List<Executor> getAllExecutors(ExecutionContext context) {
        return createAllExecutors(context);
    }

    public synchronized boolean isAllReady() {
        if (executors.isEmpty()) {
            return false;
        }
        boolean allReady = true;
        for (Executor executor : executors) {
            if (!((CacheExec) executor).isReady()) {
                allReady = false;
                break;
            }
        }
        return allReady;
    }

    private synchronized List<Executor> createAllExecutors(ExecutionContext context) {
        if (executors.isEmpty()) {
            for (int j = 0; j < parallelism; j++) {
                CacheExec executor = new CacheExec(dataTypes, context);
                executors.add(executor);
            }
        }
        return executors;
    }
}
