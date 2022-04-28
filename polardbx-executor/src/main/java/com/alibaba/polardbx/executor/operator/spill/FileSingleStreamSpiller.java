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

package com.alibaba.polardbx.executor.operator.spill;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerde;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeUtil;
import com.alibaba.polardbx.executor.mpp.execution.buffer.SerializedChunk;
import com.alibaba.polardbx.executor.mpp.util.MppIterators;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.spill.LocalSpillMonitor;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeUtil.writeSerializedChunk;
import static com.alibaba.polardbx.executor.mpp.util.MppCloseables.combineCloseables;
import static com.alibaba.polardbx.executor.operator.spill.SingleStreamSpillerFactory.SPILL_FILE_PREFIX;
import static com.alibaba.polardbx.executor.operator.spill.SingleStreamSpillerFactory.SPILL_FILE_SUFFIX;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class FileSingleStreamSpiller implements SingleStreamSpiller {
    @VisibleForTesting
    static final int BUFFER_SIZE = 4 * 1024;

    private static final Logger log = LoggerFactory.getLogger(FileSingleStreamSpiller.class);

    private static final long MAX_PAGE_BUFFER = new DataSize(2, DataSize.Unit.MEGABYTE).toBytes();

    private final FileHolder targetFile;
    private final Closer closer = Closer.create();
    private final PagesSerde serde;
    private final ArrayList<Chunk> pageBuffer = new ArrayList<>(32);

    private final ListeningExecutorService executor;
    private final FileCleaner fileCleaner;

    private boolean writable = true;
    private ListenableFuture<?> spillInProgress = NOT_BLOCKED;
    private long bufferedPageBytes;

    private final LinkedList<Iterator<Chunk>> iterators = new LinkedList<>();
    private final Object lock = new Object();
    private final Object writeObjLock = new Object();
    private boolean closed;
    private LocalSpillMonitor spillMonitor;

    public FileSingleStreamSpiller(
        PagesSerde serde,
        ListeningExecutorService executor,
        FileCleaner fileCleaner,
        Path spillPath,
        LocalSpillMonitor spillMonitor) {
        this.serde = requireNonNull(serde, "serde is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.fileCleaner = requireNonNull(fileCleaner, "fileCleaner is null");
        try {
            this.targetFile = closer.register(new FileHolder(
                Files.createTempFile(spillPath, SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX), fileCleaner));
        } catch (IOException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_SPILL, e, "Failed to create spill file");
        }
        this.spillMonitor = spillMonitor;
    }

    @Override
    public ListenableFuture<?> spill(Chunk page) {
        requireNonNull(page, "page is null");
        checkNoSpillInProgress();
        synchronized (lock) {
            if (closed) {
                return NOT_BLOCKED;
            }
            bufferedPageBytes += page.getSizeInBytes();
            pageBuffer.add(page);
            if (bufferedPageBytes > MAX_PAGE_BUFFER) {
                return flushPageBuffer();
            }
        }
        return NOT_BLOCKED;
    }

    private ListenableFuture<?> flushPageBuffer() {
        spillInProgress = executor.submit(() -> {
            try {
                writePages(pageBuffer.iterator());
            } finally {
                pageBuffer.clear();
                bufferedPageBytes = 0L;
            }
        });
        return spillInProgress;
    }

    @Override
    public ListenableFuture<?> spill(Iterator<Chunk> pageIterator) {
        requireNonNull(pageIterator, "pageIterator is null");
        checkNoSpillInProgress();
        synchronized (lock) {
            if (closed) {
                return NOT_BLOCKED;
            }
            iterators.add(pageIterator);
        }
        spillInProgress = executor.submit(() -> writePages(pageIterator));
        return spillInProgress;
    }

    @Override
    public Iterator<Chunk> getSpilledChunks() {
        checkNoSpillInProgress();
        return readPages();
    }

    @Override
    public ListenableFuture<List<Chunk>> getAllSpilledChunks() {
        return executor.submit(() -> ImmutableList.copyOf(getSpilledChunks()));
    }

    @Override
    public void flush() {
        checkNoSpillInProgress();
        synchronized (lock) {
            if (!pageBuffer.isEmpty()) {
                flushPageBuffer();
                getFutureValue(spillInProgress);
                ExecUtils.checkException(spillInProgress);
            }
        }
    }

    @Override
    public void reset() {

    }

    private void writePages(Iterator<Chunk> pageIterator) {
        checkState(writable,
            "Spilling no longer allowed. The spiller has been made non-writable on first read for subsequent reads to be consistent");
        synchronized (writeObjLock) {
            try (SliceOutput output = new OutputStreamSliceOutput(targetFile.newOutputStream(APPEND), BUFFER_SIZE)) {
                while (pageIterator.hasNext() && !closed) {
                    Chunk page = pageIterator.next();
                    SerializedChunk serializedPage = serde.serialize(false, page);
                    writeSerializedChunk(output, serializedPage);
                    spillMonitor.updateBytes(serializedPage.getSizeInBytes());
                }
            } catch (UncheckedIOException | IOException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_SPILL, e, "Failed to spill pages");
            } finally {
                iterators.remove(pageIterator);
            }
        }
    }

    private Iterator<Chunk> readPages() {
        checkState(writable, "Repeated reads are disallowed to prevent potential resource leaks");
        checkState(pageBuffer.isEmpty(), "pageBuffer is not empty");
        writable = false;

        try {
            InputStream input = targetFile.newInputStream();
            Closeable resources = closer.register(combineCloseables(input, () -> {
            }));
            Iterator<Chunk> pages = PagesSerdeUtil.readPages(serde, new InputStreamSliceInput(input, BUFFER_SIZE));
            return MppIterators.closeWhenExhausted(pages, resources);
        } catch (IOException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_SPILL, e, "Failed to read spilled pages");
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            closer.register(spillMonitor);
            closer.register(() -> {
                pageBuffer.clear();
                bufferedPageBytes = 0L;
            });
            try {
                closer.close();
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_SPILL, e, "Failed to close spiller");
            }
        }
    }

    private void checkNoSpillInProgress() {
        checkState(spillInProgress.isDone(), "spill in progress");
    }
}