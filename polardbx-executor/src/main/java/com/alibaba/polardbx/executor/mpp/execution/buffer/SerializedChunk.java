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

import com.alibaba.polardbx.optimizer.chunk.Chunk;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SerializedChunk {

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SerializedChunk.class).instanceSize();
    private static final int PAGE_COMPRESSION_SIZE = ClassLayout.parseClass(ChunkCompression.class).instanceSize();

    private final Slice slice;
    private final ChunkCompression compression;
    private final int positionCount;
    private final int uncompressedSizeInBytes;
    private final Chunk page;
    private final int length;

    public SerializedChunk(Slice slice, ChunkCompression compression, int positionCount, int uncompressedSizeInBytes) {
        this.slice = requireNonNull(slice, "slice is null");
        this.compression = requireNonNull(compression, "compression is null");
        this.positionCount = positionCount;
        checkArgument(uncompressedSizeInBytes >= 0, "uncompressedSizeInBytes is negative");
        checkArgument(compression == ChunkCompression.UNCOMPRESSED || uncompressedSizeInBytes > slice.length(),
            "compressed size must be smaller than uncompressed size when compressed");
        checkArgument(compression == ChunkCompression.COMPRESSED || uncompressedSizeInBytes == slice.length(),
            "uncompressed size must be equal to slice length when uncompressed");
        this.uncompressedSizeInBytes = uncompressedSizeInBytes;
        this.page = null;
        this.length = 0;
    }

    public SerializedChunk(Chunk page, int length, ChunkCompression compression, int positionCount) {
        this.slice = null;
        this.compression = requireNonNull(compression, "compression is null");
        this.positionCount = positionCount;
        this.uncompressedSizeInBytes = length;
        this.page = page;
        this.length = length;
    }

    public int getSizeInBytes() {
        if (page != null) {
            return length;
        }
        return slice.length();
    }

    public int getUncompressedSizeInBytes() {
        return uncompressedSizeInBytes;
    }

    public int getRetainedSizeInBytes() {
        if (page != null) {
            return INSTANCE_SIZE + (int) page.estimateSize() + PAGE_COMPRESSION_SIZE;
        }
        return INSTANCE_SIZE + slice.getRetainedSize() + PAGE_COMPRESSION_SIZE;
    }

    public int getPositionCount() {
        return positionCount;
    }

    public Slice getSlice() {
        return slice;
    }

    public Chunk getPage() {
        return page;
    }

    public ChunkCompression getCompression() {
        return compression;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("positionCount", positionCount)
            .add("compression", compression)
            .add("page", page != null)
            .add("sizeInBytes", getSizeInBytes())
            .add("uncompressedSizeInBytes", uncompressedSizeInBytes)
            .toString();
    }
}
