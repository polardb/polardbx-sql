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

package com.alibaba.polardbx.executor.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MockExec implements Executor {

    private static final String TABLE_NAME = "MOCK_TABLE";

    protected final List<DataType> columnTypes;
    private final List<Chunk> chunks;

    private boolean opened = false;
    private int currentChunkIndex = 0;

    private boolean isFinish;

    public MockExec(List<DataType> columnTypes, List<Chunk> chunks) {
        this.columnTypes = columnTypes;
        this.chunks = chunks;
    }

    public List<Chunk> getChunks() {
        return chunks;
    }

    @Override
    public void open() {
        if (opened) {
            throw new AssertionError("opened twice");
        } else {
            opened = true;
        }
    }

    @Override
    public Chunk nextChunk() {
        if (!opened) {
            throw new AssertionError("not opened");
        }
        if (currentChunkIndex < chunks.size()) {
            return chunks.get(currentChunkIndex++);
        } else {
            isFinish = true;
            return null;
        }
    }

    @Override
    public void close() {
        if (!opened) {
            throw new AssertionError("not opened");
        }
    }

    @Override
    public List<DataType> getDataTypes() {
        return columnTypes;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    public static MockExecBuilder builder(DataType... columnTypes) {
        return new MockExecBuilder(Arrays.asList(columnTypes));
    }

    @Override
    public boolean produceIsFinished() {
        return isFinish;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }

    public static class MockExecBuilder {

        private List<DataType> columnTypes;
        private List<Chunk> chunks = new ArrayList<>();

        private MockExecBuilder(List<DataType> columnTypes) {
            this.columnTypes = columnTypes;
        }

        public MockExecBuilder withChunk(Chunk chunk) {
            chunks.add(chunk);
            return this;
        }

        public MockExec build() {
            return new MockExec(columnTypes, chunks);
        }
    }
}
