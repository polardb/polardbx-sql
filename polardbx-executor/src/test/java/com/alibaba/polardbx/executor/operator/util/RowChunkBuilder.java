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
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RowChunkBuilder {
    private final List<DataType> types;

    public static RowChunkBuilder rowChunkBuilder(DataType... types) {
        return rowChunkBuilder(ImmutableList.copyOf(types));
    }

    public static RowChunkBuilder rowChunkBuilder(Iterable<DataType> types) {
        return new RowChunkBuilder(types);
    }

    private final List<BlockBuilder> builders;
    private long rowCount;

    RowChunkBuilder(Iterable<DataType> types) {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        ImmutableList.Builder<BlockBuilder> builders = ImmutableList.builder();
        for (DataType type : types) {
            builders.add(BlockBuilders.create(type, new ExecutionContext()));
        }
        this.builders = builders.build();
        checkArgument(!this.builders.isEmpty(), "At least one value info is required");
    }

    public boolean isEmpty() {
        return rowCount == 0;
    }

    public RowChunkBuilder row(Object... values) {
        checkArgument(values.length == builders.size(), "Expected %s values, but got %s", builders.size(),
            values.length);

        for (int channel = 0; channel < values.length; channel++) {
            append(channel, values[channel]);
        }
        rowCount++;
        return this;
    }

    public Chunk build() {
        Block[] blocks = new Block[builders.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = builders.get(i).build();
        }
        return new Chunk(blocks);
    }

    private void append(int channel, Object element) {
        BlockBuilder blockBuilder = builders.get(channel);
        blockBuilder.writeObject(element);
    }
}