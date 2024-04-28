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

package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.IntegerReader;
import org.apache.orc.impl.RunLengthIntegerReader;
import org.apache.orc.impl.RunLengthIntegerReaderV2;
import org.apache.orc.impl.StreamName;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.StampedLock;

/**
 * A column reader is responsible for column-level parsing and reading.
 * It consists of several input streams.
 * There are seekBytes and seekRow method because the element position
 * and byte location are not aligned due to compression.
 */
public interface ColumnReader {
    String COLUMN_READER_MEMORY = "ColumnReader.Memory";
    String COLUMN_READER_TIMER = "ColumnReader.Timer";

    /**
     * Release a reference of this column reader.
     */
    void releaseRef(int decrement);

    /**
     * Retain a reference of this column reader.
     */
    void retainRef(int increment);

    /**
     * Get the count of reference.
     */
    int refCount();

    void setNoMoreBlocks();

    boolean hasNoMoreBlocks();

    StampedLock getLock();

    /**
     * Get the row group included in this reader.
     */
    boolean[] rowGroupIncluded();

    /**
     * Check if the resource of this column reader has been opened.
     */
    boolean isOpened();

    /**
     * Open the resource of column reader.
     *
     * @param await Whether we wait for IO results synchronously.
     * @param rowGroupIncluded The row group bitmap used to load the stream data. The value is null means
     * all row groups will be loaded.
     */
    void open(boolean await, boolean[] rowGroupIncluded);

    /**
     * Open the column-reader use a built IO tasks whose result is mapping from stream-name to InStream object.
     *
     * @param loadFuture future of IO tasks.
     * @param await Whether we wait for IO results synchronously.
     * @param rowGroupIncluded The row group bitmap used to load the stream data. The value is null means
     * all row groups will be loaded.
     */
    void open(CompletableFuture<Map<StreamName, InStream>> loadFuture, boolean await, boolean[] rowGroupIncluded);

    /**
     * Set the starting position for the next reading process
     *
     * @param rowGroupId starting row group.
     * @param elementPosition starting element position.
     */
    void startAt(int rowGroupId, int elementPosition) throws IOException;

    /**
     * NOTE: use fix length block to reading parsed data.
     * After seeking the byte location and row position, fill the given block
     * from the last position with given position count.
     *
     * @param positionCount the position count to fill.
     */
    default void next(RandomAccessBlock randomAccessBlock, int positionCount) throws IOException {
        throw new UnsupportedOperationException();
    }

    default int next(RandomAccessBlock randomAccessBlock, int positionCount, int[] selection, int selSize)
        throws IOException {
        next(randomAccessBlock, positionCount);
        return 0;
    }

    /**
     * Release the buffers in this column reader.
     */
    void close();

    boolean needCache();

    boolean isClosed();

    static IntegerReader createIntegerReader(InStream dataStream, OrcProto.ColumnEncoding.Kind kind)
        throws IOException {
        return createIntegerReader(dataStream, kind, true);
    }

    static IntegerReader createIntegerReader(InStream dataStream, OrcProto.ColumnEncoding.Kind kind, boolean signed)
        throws IOException {
        switch (kind) {
        case DIRECT_V2:
        case DICTIONARY_V2:
            return new RunLengthIntegerReaderV2(dataStream, signed, true);
        case DIRECT:
        case DICTIONARY:
            return new RunLengthIntegerReader(dataStream, signed);
        default:
            throw GeneralUtil.nestedException("Unknown encoding " + kind);
        }
    }
}
