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

package com.alibaba.polardbx.executor.mpp.execution.buffer;

import com.alibaba.polardbx.executor.chunk.BlockEncoding;
import com.alibaba.polardbx.executor.chunk.BlockEncodingBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.compress.lz4.Lz4RawCompressor.maxCompressedLength;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class PagesSerde {
    private static final double MINIMUM_COMPRESSION_RATIO = 0.8;

    private final Optional<Compressor> compressor;
    private final Optional<Decompressor> decompressor;
    private final List<BlockEncoding> blockEncodings;

    public PagesSerde(Optional<Compressor> compressor,
                      Optional<Decompressor> decompressor,
                      List<DataType> types) {
        this.compressor = requireNonNull(compressor, "compressor is null");
        this.decompressor = requireNonNull(decompressor, "decompressor is null");
        this.blockEncodings = BlockEncodingBuilders.create(types);
        checkArgument(compressor.isPresent() == decompressor.isPresent(),
            "compressor and decompressor must both be present or both be absent");
    }

    public SerializedChunk serialize(boolean localChunk, Chunk page) {
        if (localChunk) {
            return new SerializedChunk(page, (int) page.getSizeInBytes(), ChunkCompression.UNCOMPRESSED,
                page.getPositionCount());
        }
        return serializeForce(page);
    }

    private SerializedChunk serializeForce(Chunk page) {
        SliceOutput serializationBuffer =
            new DynamicSliceOutput(toIntExact((page.getSizeInBytes() + Integer.BYTES))); // block
        // length is an int
        PagesSerdeUtil.writeRawPage(page, serializationBuffer, blockEncodings);

        if (!compressor.isPresent()) {
            return new SerializedChunk(serializationBuffer.slice(), ChunkCompression.UNCOMPRESSED,
                page.getPositionCount(), serializationBuffer.size());
        }

        int maxCompressedLength = maxCompressedLength(serializationBuffer.size());
        byte[] compressionBuffer = new byte[maxCompressedLength];
        int actualCompressedLength = compressor.get()
            .compress(serializationBuffer.slice().getBytes(), 0, serializationBuffer.size(), compressionBuffer, 0,
                maxCompressedLength);

        if (((1.0 * actualCompressedLength) / serializationBuffer.size()) > MINIMUM_COMPRESSION_RATIO) {
            return new SerializedChunk(serializationBuffer.slice(), ChunkCompression.UNCOMPRESSED,
                page.getPositionCount(), serializationBuffer.size());
        }

        return new SerializedChunk(
            Slices.copyOf(Slices.wrappedBuffer(compressionBuffer, 0, actualCompressedLength)),
            ChunkCompression.COMPRESSED,
            page.getPositionCount(),
            serializationBuffer.size());
    }

    public Chunk deserialize(SerializedChunk serializedChunk) {
        checkArgument(serializedChunk != null, "serializedChunk is null");

        if (serializedChunk.getPage() != null) {
            return serializedChunk.getPage();
        }

        if (!decompressor.isPresent() || serializedChunk.getCompression() == ChunkCompression.UNCOMPRESSED) {
            return PagesSerdeUtil.readRawPage(serializedChunk.getPositionCount(), serializedChunk.getSlice().getInput(),
                blockEncodings);
        }

        int uncompressedSize = serializedChunk.getUncompressedSizeInBytes();
        byte[] decompressed = new byte[uncompressedSize];
        int actualUncompressedSize = decompressor.get()
            .decompress(serializedChunk.getSlice().getBytes(), 0, serializedChunk.getSlice().length(), decompressed, 0,
                uncompressedSize);
        checkState(uncompressedSize == actualUncompressedSize);

        return PagesSerdeUtil.readRawPage(serializedChunk.getPositionCount(), Slices
            .wrappedBuffer(decompressed, 0, uncompressedSize).getInput(), blockEncodings);
    }
}
