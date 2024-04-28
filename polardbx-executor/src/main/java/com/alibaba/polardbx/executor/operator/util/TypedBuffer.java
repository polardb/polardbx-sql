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

import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.BitSet;
import java.util.List;

public interface TypedBuffer<T> {
    void appendRow(T array, int nullPosition, int positionCount);

    default void appendRow(T array, int positionCount) {
        appendRow(array, -1, positionCount);
    }

    void appendRow(Chunk chunk, int position);

    List<Chunk> buildChunks();

    boolean equals(int position, Chunk otherChunk, int otherPosition);

    void appendValuesTo(int position, ChunkBuilder chunkBuilder);

    long estimateSize();

    // type-specific interface
    void appendInteger(int col, int value);

    void appendLong(int col, long value);

    void appendNull(int col);

    static TypedBuffer create(DataType[] dataTypes, int chunkSize, ExecutionContext context) {
        BlockBuilder[] blockBuilders = new BlockBuilder[dataTypes.length];
        for (int i = 0; i < dataTypes.length; i++) {
            blockBuilders[i] = BlockBuilders.create(dataTypes[i], context);
        }
        return new DefaultTypedBuffer(blockBuilders, chunkSize, context);
    }

    static TypedBuffer createTypeSpecific(DataType dataType, int chunkSize, ExecutionContext context) {
        MySQLStandardFieldType fieldType = dataType.fieldType();
        switch (fieldType) {
        case MYSQL_TYPE_LONGLONG:
            return TypedBuffers.createLong(chunkSize, context);
        case MYSQL_TYPE_LONG:
            return TypedBuffers.createInt(chunkSize, context);
        default: {
            // fall back
            return create(new DataType[] {dataType}, chunkSize, context);
        }
        }
    }
}
