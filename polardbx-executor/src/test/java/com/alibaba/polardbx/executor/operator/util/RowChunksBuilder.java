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

package com.alibaba.polardbx.executor.operator.util;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.BucketMockExec;
import com.alibaba.polardbx.executor.operator.MockExec;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.alibaba.polardbx.executor.operator.util.RowChunkBuilder.rowChunkBuilder;
import static java.util.Objects.requireNonNull;

public class RowChunksBuilder {
    public static RowChunksBuilder rowChunksBuilder(DataType... types) {
        return rowChunksBuilder(ImmutableList.copyOf(types));
    }

    public static RowChunksBuilder rowChunksBuilder(Iterable<DataType> types) {
        return new RowChunksBuilder(types);
    }

    public static RowChunksBuilder rowChunksBuilder(boolean hashEnabled, List<Integer> hashChannels,
                                                    DataType... types) {
        return rowChunksBuilder(hashEnabled, hashChannels, ImmutableList.copyOf(types));
    }

    public static RowChunksBuilder rowChunksBuilder(boolean hashEnabled, List<Integer> hashChannels,
                                                    Iterable<DataType> types) {
        return new RowChunksBuilder(hashEnabled, Optional.of(hashChannels), types);
    }

    private final ImmutableList.Builder<Chunk> chunks = ImmutableList.builder();
    private final List<DataType> types;
    private RowChunkBuilder builder;
    private final Optional<List<Integer>> hashChannels;

    RowChunksBuilder(Iterable<DataType> types) {
        this(false, Optional.empty(), types);
    }

    RowChunksBuilder(boolean hashEnabled, Optional<List<Integer>> hashChannels, Iterable<DataType> types) {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.hashChannels = hashChannels;
        builder = rowChunkBuilder(types);
    }

    public RowChunksBuilder addSequenceChunk(int length, int... initialValues) {
        checkArgument(length > 0, "length must be at least 1");
        requireNonNull(initialValues, "initialValues is null");
        checkArgument(initialValues.length == types.size(), "Expected %s initialValues, but got %s", types.size(),
            initialValues.length);

        chunkBreak();
        Chunk chunk = SequenceChunkBuilder.createSequenceChunk(types, length, initialValues);
        chunks.add(chunk);
        return this;
    }

    public RowChunksBuilder addBlocksChunk(Block... blocks) {
        chunks.add(new Chunk(blocks));
        return this;
    }

    public RowChunksBuilder row(Object... values) {
        builder.row(values);
        return this;
    }

    public RowChunksBuilder rows(Object... rows) {
        for (Object row : rows) {
            row(row);
        }
        return this;
    }

    public RowChunksBuilder rows(Object[]... rows) {
        for (Object[] row : rows) {
            row(row);
        }
        return this;
    }

    public RowChunksBuilder chunkBreak() {
        if (!builder.isEmpty()) {
            chunks.add(builder.build());
            builder = rowChunkBuilder(types);
        }
        return this;
    }

    public List<Chunk> build() {
        chunkBreak();
        List<Chunk> resultChunks = chunks.build();
        return resultChunks;
    }

    public MockExec buildExec() {
        chunkBreak();
        List<Chunk> resultChunks = chunks.build();
        return new MockExec(types, resultChunks);
    }

    public MockExec buildBucketExec(int bucketNum, List<Integer> partitionChannels) {
        chunkBreak();
        List<Chunk> resultChunks = chunks.build();
        return new BucketMockExec(types, resultChunks, bucketNum, partitionChannels);
    }

    public List<DataType> getTypes() {
        return types;
    }
}
