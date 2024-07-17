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

package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.ProducerExecutor;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public class EmptyExecutor implements Executor {

    private List<DataType> columnMetas;
    private static List<Executor> EMPTY = ImmutableList.of();

    public EmptyExecutor(List<DataType> columnMetas) {
        this.columnMetas = columnMetas;
    }

    @Override
    public void open() {

    }

    @Override
    public Chunk nextChunk() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public List<DataType> getDataTypes() {
        return columnMetas;
    }

    @Override
    public List<Executor> getInputs() {
        return EMPTY;
    }

    @Override
    public int getId() {
        return -1;
    }

    @Override
    public boolean produceIsFinished() {
        return true;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }
}
