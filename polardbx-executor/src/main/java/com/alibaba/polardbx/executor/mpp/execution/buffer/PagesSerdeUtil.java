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

import com.alibaba.polardbx.optimizer.chunk.Block;
import com.alibaba.polardbx.optimizer.chunk.BlockEncoding;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.clearspring.analytics.util.AbstractIterator;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Iterator;
import java.util.List;

import static com.alibaba.polardbx.executor.mpp.execution.buffer.ChunkCompression.lookupCodecFromMarker;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PagesSerdeUtil {

    public static void writeRawPage(Chunk page, SliceOutput output, List<BlockEncoding> blockEncodings) {
        output.writeInt(page.getBlockCount());
        for (int i = 0; i < page.getBlockCount(); i++) {
            blockEncodings.get(i).writeBlock(output, page.getBlock(i));
        }
    }

    public static Chunk readRawPage(int positionCount, SliceInput input, List<BlockEncoding> blockEncodings) {
        int numberOfBlocks = input.readInt();
        Block[] blocks = new Block[numberOfBlocks];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = blockEncodings.get(i).readBlock(input);
        }
        return new Chunk(positionCount, blocks);
    }

    public static long writeSerializedChunk(SliceOutput output, SerializedChunk page) {
        output.writeInt(page.getPositionCount());
        output.writeByte(page.getCompression().getMarker());
        output.writeInt(page.getUncompressedSizeInBytes());
        output.writeInt(page.getSizeInBytes());
        output.writeBytes(page.getSlice());
        return Integer.BYTES * 3 + Byte.BYTES + page.getSlice().length();
    }

    private static SerializedChunk readSerializedChunk(SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        byte codecMarker = sliceInput.readByte();
        int uncompressedSizeInBytes = sliceInput.readInt();
        int sizeInBytes = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(toIntExact((sizeInBytes)));
        return new SerializedChunk(slice, lookupCodecFromMarker(codecMarker), positionCount, uncompressedSizeInBytes);
    }

    public static long writeSerializedChunks(SliceOutput sliceOutput, Iterable<SerializedChunk> pages) {
        Iterator<SerializedChunk> pageIterator = pages.iterator();
        long size = 0;
        while (pageIterator.hasNext()) {
            SerializedChunk page = pageIterator.next();
            writeSerializedChunk(sliceOutput, page);
            size += page.getSizeInBytes();
        }
        return size;
    }

    public static Iterator<SerializedChunk> readSerializedChunks(SliceInput sliceInput) {
        return new SerializedChunkReader(sliceInput);
    }

    private static class SerializedChunkReader
        extends AbstractIterator<SerializedChunk> {
        private final SliceInput input;

        SerializedChunkReader(SliceInput input) {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        protected SerializedChunk computeNext() {
            if (!input.isReadable()) {
                return endOfData();
            }

            return readSerializedChunk(input);
        }
    }

    public static long writeChunk(PagesSerde serde, SliceOutput sliceOutput, Chunk page) {
        return writeSerializedChunk(sliceOutput, serde.serialize(false, page));
    }

    public static Iterator<Chunk> readPages(PagesSerde serde, SliceInput sliceInput) {
        return new PageReader(serde, sliceInput);
    }

    private static class PageReader
        extends com.google.common.collect.AbstractIterator<Chunk> {
        private final PagesSerde serde;
        private final SliceInput input;

        PageReader(PagesSerde serde, SliceInput input) {
            this.serde = requireNonNull(serde, "serde is null");
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        protected Chunk computeNext() {
            if (!input.isReadable()) {
                return endOfData();
            }
            SerializedChunk serializedChunk = readSerializedChunk(input);
            return serde.deserialize(serializedChunk);
        }
    }
}