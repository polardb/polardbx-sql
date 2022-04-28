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

import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.ArrayList;
import java.util.List;

public interface Benchmark {

    void run();

    class Timer {
        private final int iteration;
        private Long accumulatedTime = 0L;
        private Long timeStart = 0L;

        public Timer(int iteration) {
            this.iteration = iteration;
        }

        public void startTiming() {
            timeStart = System.currentTimeMillis();
        }

        public void stopTiming() {
            accumulatedTime += System.currentTimeMillis() - timeStart;
            timeStart = 0L;
        }

        public long totalTime() {
            return accumulatedTime;
        }
    }

    class MockIntegerExec implements Executor {

        private static final String TABLE_NAME = "MOCK_TABLE";

        protected List<DataType> columnTypes;

        private int start;
        private int end;

        private boolean opened = false;
        private int chunkSize = 1024;

        public MockIntegerExec(int start, int end) {
            this.start = start;
            this.end = end;
            this.columnTypes = ImmutableList.of(DataTypes.IntegerType);
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
            if (start >= end) {
                return null;
            }
            int[] values = new int[chunkSize];
            for (int i = 0; i < chunkSize; i++) {
                values[i] = start++;
            }
            IntegerBlock block = IntegerBlock.wrap(values);
            return new Chunk(block);
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

        @Override
        public boolean produceIsFinished() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<?> produceIsBlocked() {
            throw new UnsupportedOperationException();
        }
    }

    class MockOverFlowExec implements Executor {

        private static final String TABLE_NAME = "MOCK_TABLE";

        protected List<DataType> columnTypes;

        private boolean opened = false;
        private int chunkSize = 1024;
        private int runCount = 0;
        private static Chunk overFlowChunk = null;
        private static Chunk nonOverFlowChunk = null;
        private boolean overflow;

        static {
            long[] values = new long[1024];
            long[] values2 = new long[1024];
            int[] ids = new int[1024];
            for (int i = 0; i < 1024; i++) {
                values[i] = i % 4 + Long.MAX_VALUE - 1000 + (long) (Math.random() * 1000);
                values2[i] = i % 4;
                ids[i] = i;
            }
            LongBlock block = LongBlock.wrap(values);
            LongBlock block2 = LongBlock.wrap(values2);
            IntegerBlock idBlock = IntegerBlock.wrap(ids);
            overFlowChunk = new Chunk(idBlock, block);
            nonOverFlowChunk = new Chunk(idBlock, block2);
        }

        public MockOverFlowExec(boolean overflow) {
            this.overflow = overflow;
            this.columnTypes = ImmutableList.of(DataTypes.IntegerType, DataTypes.LongType);
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
            if (runCount > 1024 * 10) {
                return null;
            }
            runCount++;
            return overflow ? overFlowChunk : nonOverFlowChunk;
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

        @Override
        public boolean produceIsFinished() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<?> produceIsBlocked() {
            throw new UnsupportedOperationException();
        }
    }

    class MockMultiKeysExec implements Executor {

        private static final String TABLE_NAME = "MOCK_TABLE";

        protected List<ColumnMeta> cachedColumnMeta;

        protected List<DataType> columnTypes;

        private int start;
        private int end;

        private boolean opened = false;
        private int chunkSize = 1024;

        private List<int[]> values = new ArrayList<>();
        private int maxValue;

        public MockMultiKeysExec(int start, int end, int columnNum) {
            this(start, end, columnNum, 65535);
        }

        public MockMultiKeysExec(int start, int end, int columnNum, int maxValue) {
            this.start = start;
            this.end = end;
            this.maxValue = maxValue;

            this.columnTypes = new ArrayList<>(columnNum);
            for (int i = 0; i < columnNum; i++) {
                columnTypes.add(DataTypes.IntegerType);
                values.add(new int[chunkSize]);
            }
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
            if (start >= end) {
                return null;
            }

            for (int i = 0; i < chunkSize; i++) {
                int value = start & maxValue;
                for (int j = 0; j < values.size(); j++) {
                    values.get(j)[i] = value;
                }
                start++;
            }
            IntegerBlock[] blocks = new IntegerBlock[values.size()];
            for (int j = 0; j < values.size(); j++) {
                blocks[j] = IntegerBlock.wrap(values.get(j));
            }
            return new Chunk(blocks);
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

        @Override
        public boolean produceIsFinished() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<?> produceIsBlocked() {
            throw new UnsupportedOperationException();
        }
    }
}
