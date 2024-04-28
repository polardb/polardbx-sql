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

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.operator.scan.AbstractColumnReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.MetricsNameBuilder;
import com.alibaba.polardbx.executor.operator.scan.metrics.ORCMetricsWrapper;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileKeys;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import org.apache.orc.OrcProto;
import org.apache.orc.customized.ORCProfile;
import org.apache.orc.impl.BitFieldReader;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.IntegerReader;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.RunLengthIntegerReaderV2;
import org.apache.orc.impl.StreamName;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractLongColumnReader extends AbstractColumnReader {
    // basic metadata
    protected final StripeLoader stripeLoader;

    // in preheat mode, all row-indexes in orc-index should not be null.
    protected final OrcIndex orcIndex;
    protected final RuntimeMetrics metrics;

    protected final OrcProto.ColumnEncoding.Kind kind;
    protected final int indexStride;

    protected final boolean enableMetrics;

    // open parameters
    protected boolean[] rowGroupIncluded;
    protected boolean await;

    // inner states
    protected AtomicBoolean openFailed;
    protected AtomicBoolean initializeOnlyOnce;
    protected AtomicBoolean isOpened;

    // IO results
    protected Throwable throwable;
    protected Map<StreamName, InStream> inStreamMap;
    protected CompletableFuture<Map<StreamName, InStream>> openFuture;

    // for semantic parser
    protected BitFieldReader present;
    protected IntegerReader data;

    // record read positions
    protected int currentRowGroup;
    protected int lastPosition;

    // execution time metrics.
    protected Counter preparingTimer;
    protected Counter seekTimer;
    protected Counter parseTimer;

    public AbstractLongColumnReader(int columnId, boolean isPrimaryKey, StripeLoader stripeLoader, OrcIndex orcIndex,
                                    RuntimeMetrics metrics, OrcProto.ColumnEncoding.Kind kind, int indexStride,
                                    boolean enableMetrics) {
        super(columnId, isPrimaryKey);
        this.stripeLoader = stripeLoader;
        this.orcIndex = orcIndex;
        this.metrics = metrics;
        this.kind = kind;
        this.indexStride = indexStride;
        this.enableMetrics = enableMetrics;

        // inner states
        openFailed = new AtomicBoolean(false);
        initializeOnlyOnce = new AtomicBoolean(false);
        isOpened = new AtomicBoolean(false);
        throwable = null;
        inStreamMap = null;
        openFuture = null;

        // for parser
        present = null;
        data = null;

        // read position control
        // The initial value is -1 means it must seek to the correct row group firstly.
        currentRowGroup = -1;
        lastPosition = -1;

        rowGroupIncluded = null;
        await = false;

        if (enableMetrics) {
            preparingTimer = metrics.addCounter(
                MetricsNameBuilder.columnMetricsKey(columnId, ProfileKeys.ORC_COLUMN_IO_PREPARING_TIMER),
                COLUMN_READER_TIMER,
                ProfileKeys.ORC_COLUMN_IO_PREPARING_TIMER.getProfileUnit()
            );

            seekTimer = metrics.addCounter(
                MetricsNameBuilder.columnMetricsKey(columnId, ProfileKeys.ORC_COLUMN_SEEK_TIMER),
                COLUMN_READER_TIMER,
                ProfileKeys.ORC_COLUMN_SEEK_TIMER.getProfileUnit()
            );

            parseTimer = metrics.addCounter(
                MetricsNameBuilder.columnMetricsKey(columnId, ProfileKeys.ORC_COLUMN_PARSE_TIMER),
                COLUMN_READER_TIMER,
                ProfileKeys.ORC_COLUMN_PARSE_TIMER.getProfileUnit()
            );
        }

    }

    @Override
    public boolean[] rowGroupIncluded() {
        Preconditions.checkArgument(isOpened.get());
        return rowGroupIncluded;
    }

    @Override
    public boolean isOpened() {
        return isOpened.get();
    }

    @Override
    public void open(boolean await, boolean[] rowGroupIncluded) {
        if (!isOpened.compareAndSet(false, true)) {
            throw GeneralUtil.nestedException("It's not allowed to re-open this column reader.");
        }
        this.rowGroupIncluded = rowGroupIncluded;
        this.await = await;

        // load the specified streams.
        openFuture = stripeLoader.load(columnId, rowGroupIncluded);

        if (await) {
            doWait();
        }
    }

    @Override
    public void open(CompletableFuture<Map<StreamName, InStream>> loadFuture,
                     boolean await, boolean[] rowGroupIncluded) {
        if (!isOpened.compareAndSet(false, true)) {
            throw GeneralUtil.nestedException("It's not allowed to re-open this column reader.");
        }
        this.rowGroupIncluded = rowGroupIncluded;
        this.await = await;
        this.openFuture = loadFuture;
        if (await) {
            doWait();
        }
    }

    // wait for open future and handle failure.
    private void doWait() {
        try {
            inStreamMap = openFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        if (throwable != null) {
            // throw if failed.
            throw GeneralUtil.nestedException(throwable);
        }
    }

    protected void init() throws IOException {
        if (!initializeOnlyOnce.compareAndSet(false, true)) {
            return;
        }

        long start = System.nanoTime();
        if (!await) {
            doWait();
        }
        if (openFailed.get()) {
            return;
        }

        // Unlike StripePlanner in raw ORC SDK, the stream names and IO production are
        // all determined at runtime.
        StreamName presentName = new StreamName(columnId, OrcProto.Stream.Kind.PRESENT);
        StreamName dataName = new StreamName(columnId, OrcProto.Stream.Kind.DATA);

        InStream presentStream = inStreamMap.get(presentName);
        InStream dataStream = inStreamMap.get(dataName);

        // initialize present and integer reader
        present = presentStream == null ? null : new BitFieldReader(presentStream);
        data = dataStream == null ? null : ColumnReader.createIntegerReader(dataStream, kind);

        // Add memory metrics.
        if (present != null) {
            String metricsName = MetricsNameBuilder.streamMetricsKey(
                presentName, ProfileKeys.ORC_STREAM_READER_MEMORY_COUNTER
            );

            ORCProfile memoryCounter = enableMetrics ? new ORCMetricsWrapper(
                metricsName,
                COLUMN_READER_MEMORY,
                ProfileKeys.ORC_STREAM_READER_MEMORY_COUNTER.getProfileUnit(),
                metrics
            ) : null;

            present.setMemoryCounter(memoryCounter);
        }

        if (data != null && data instanceof RunLengthIntegerReaderV2) {
            String metricsName = MetricsNameBuilder.streamMetricsKey(
                dataName, ProfileKeys.ORC_STREAM_READER_MEMORY_COUNTER
            );

            ORCProfile memoryCounter = enableMetrics ? new ORCMetricsWrapper(
                metricsName,
                COLUMN_READER_MEMORY,
                ProfileKeys.ORC_STREAM_READER_MEMORY_COUNTER.getProfileUnit(),
                metrics
            ) : null;

            ((RunLengthIntegerReaderV2) data).setMemoryCounter(memoryCounter);
        }

        // metrics time cost of preparing (IO waiting + data steam reader constructing)
        if (enableMetrics) {
            preparingTimer.inc(System.nanoTime() - start);
        }
    }

    @Override
    public void startAt(int rowGroupId, int elementPosition) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        Preconditions.checkArgument(rowGroupIncluded[rowGroupId]);
        init();

        long start = System.nanoTime();

        // case 1: the column-reader has not been accessed,
        // and the first access is the first effective row-group and the position is 0.
        boolean isFirstAccess = (currentRowGroup == -1 && lastPosition == -1)
            && elementPosition == 0
            && rowGroupId == 0;

        // case 2: the next access follows the last position in the same row-group.
        boolean isConsecutive = rowGroupId == currentRowGroup && elementPosition == lastPosition;

        // case 3: the last access reach the last position of the row-group, and the next access is the next
        // valid row-group starting at position 0.
        boolean isNextRowGroup = currentRowGroup < rowGroupId
            && elementPosition == 0
            && lastPosition == indexStride
            && (currentRowGroup + 1 == rowGroupId);

//        boolean isFirstAccess = (currentRowGroup == -1 && lastPosition == -1)
//            && elementPosition == 0
//            && ColumnReader.countTrue(rowGroupIncluded, 0, rowGroupId) == 0;

//        boolean isNextRowGroup = currentRowGroup < rowGroupId
//            && elementPosition == 0
//            && lastPosition == indexStride
//            && ((currentRowGroup + 1 == rowGroupId)
//            || (ColumnReader.countTrue(rowGroupIncluded, currentRowGroup + 1, rowGroupId) == 0));

        // It's in order.
        if (isFirstAccess || isConsecutive || isNextRowGroup) {
            lastPosition = elementPosition;
            currentRowGroup = rowGroupId;
            // metrics
            if (enableMetrics) {
                seekTimer.inc(System.nanoTime() - start);
            }

            return;
        }

        // It's not in order, need skip some position.
        if (rowGroupId != currentRowGroup || elementPosition < lastPosition) {
            // case 1: when given row group is different from the current group, seek to the position of it.
            // case 2: when elementPosition <= lastPosition, we need go back to the start position of this row group.
            seek(rowGroupId);

            long skipLen = skipPresent(elementPosition);

            if (data != null) {
                data.skip(skipLen);
            }

        } else if (elementPosition > lastPosition && elementPosition < indexStride) {
            // case 3: when elementPosition > lastPosition and the group is same, just skip to given position.
            long skipLen = skipPresent(elementPosition - lastPosition);

            if (data != null) {
                data.skip(skipLen);
            }

        } else if (elementPosition >= indexStride) {
            // case 4: the position is out of range.
            throw GeneralUtil.nestedException("Invalid element position: " + elementPosition);
        }
        // case 5: the elementPosition == lastPosition and rowGroupId is equal.

        lastPosition = elementPosition;
        currentRowGroup = rowGroupId;
        // metrics
        if (enableMetrics) {
            seekTimer.inc(System.nanoTime() - start);
        }
    }

    // Try to skip rows on present stream and count down
    // the actual rows need skipped by data stream.
    protected long skipPresent(long rows) throws IOException {
        if (present == null) {
            return rows;
        }

        long result = 0;
        for (long c = 0; c < rows; ++c) {
            // record the count of non-null values
            // in range of [current_position, current_position + rows)
            if (present.next() == 1) {
                result += 1;
            }
        }
        // It must be less than or equal to count of rows.
        return result;
    }

    @Override
    public void seek(int rowGroupId) throws IOException {
        Preconditions.checkArgument(isOpened.get());
        Preconditions.checkArgument(!openFailed.get());
        init();

        // Find the position-provider of given column and row group.
        PositionProvider positionProvider;
        OrcProto.RowIndex[] rowIndices = orcIndex.getRowGroupIndex();
        OrcProto.RowIndexEntry entry = rowIndices[columnId].getEntry(rowGroupId);
        // This is effectively a test for pre-ORC-569 files.
        if (rowGroupId == 0 && entry.getPositionsCount() == 0) {
            positionProvider = new RecordReaderImpl.ZeroPositionProvider();
        } else {
            positionProvider = new RecordReaderImpl.PositionProviderImpl(entry);
        }

        // Seek on present stream and data stream.
        if (present != null) {
            present.seek(positionProvider);
        }
        if (data != null) {
            data.seek(positionProvider);
        }

        currentRowGroup = rowGroupId;
        lastPosition = 0;
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        // 1. Clear the resources allocated in InStream
        StreamName presentName = new StreamName(columnId, OrcProto.Stream.Kind.PRESENT);
        StreamName dataName = new StreamName(columnId, OrcProto.Stream.Kind.DATA);

        // maybe the stripe loading task is not finished yet.
        if (inStreamMap != null) {
            InStream presentStream = inStreamMap.get(presentName);
            InStream dataStream = inStreamMap.get(dataName);

            if (presentStream != null) {
                presentStream.close();
            }

            if (dataStream != null) {
                dataStream.close();
            }
        }

        // 2. Clear the memory resources held by stream
        long releasedBytes = 0L;
        releasedBytes += stripeLoader.clearStream(presentName);
        releasedBytes += stripeLoader.clearStream(dataName);

        if (releasedBytes > 0) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(MessageFormat.format(
                    "Release the resource of work: {0}, columnId: {1}, bytes: {2}",
                    metrics.name(), columnId, releasedBytes
                ));
            }
        }
    }
}
