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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.operator.spill;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerde;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeUtil;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.SliceOutput;

import javax.annotation.concurrent.GuardedBy;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;
import static com.alibaba.polardbx.executor.operator.spill.SingleStreamSpiller.NOT_BLOCKED;
import static java.util.Objects.requireNonNull;

public class AsyncPageFileChannelWriter {
    private final FileHolder id;
    private final PagesSerde serde;
    private final WriteQueue requetsQueue;

    private final Object lock = new Object();

    @GuardedBy("lock")
    private SettableFuture<?> writeFuture = null;
    @GuardedBy("lock")
    private IOException iex;
    /**
     * closed flag:
     * true -> not accecpt new IO request, wait for all pendingRequest to be done
     */
    private volatile boolean closed;
    private SliceOutput output;

    private Runnable onClose;

    private final ArrayList<IORequest.WriteIORequest> pendingRequest = new ArrayList<>();

    private SpillMonitor spillMonitor;

    public AsyncPageFileChannelWriter(FileHolder id, PagesSerde serde, WriteQueue requetsQueue,
                                      Runnable noThroableOnClose, SpillMonitor spillMonitor)
        throws IOException {
        this.id = requireNonNull(id, "FileId is null");
        this.serde = requireNonNull(serde, "PagesSerde is null");
        this.requetsQueue = requireNonNull(requetsQueue, "runningRequests is null");
        // spiller new stream in truncate mode, so after flush, when writer write again, the file will refresh
        this.output = new OutputStreamSliceOutput(
            new BufferedOutputStream(new FileOutputStream(id.getFilePath().toFile(), false)));
        this.onClose = requireNonNull(noThroableOnClose);
        this.spillMonitor = spillMonitor;
    }

    public void close(boolean errorClose) throws IOException {
        synchronized (lock) {
            if (closed) {
                return;
            }
            try {
                if (errorClose) {
                    if (pendingRequest.size() > 0) {
                        for (IORequest.WriteIORequest request : pendingRequest) {
                            request.close();
                        }
                    }
                }
                closed = true;
                checkIOException();
                while (pendingRequest.size() > 0) {
                    try {
                        this.lock.wait(100);
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                    checkIOException();
                }
            } finally {
                this.onClose.run();
                if (output != null) {
                    output.close();
                    output = null;
                }
            }
        }
    }

    public ListenableFuture<?> writePage(Chunk page) throws IOException {
        return addRequest(new PageWriteRequest(page), page.estimateSize());
    }

    public ListenableFuture<?> writePages(Iterator<Chunk> pages) throws IOException {
        PagesWriteRequest request = new PagesWriteRequest(pages);
        // addRequest must return NOT_BLOCKED
        addRequest(request, -1);
        return request.getWriteFuture();
    }

    private ListenableFuture addRequest(IORequest.WriteIORequest request, long bytes)
        throws IOException {
        synchronized (lock) {
            checkIOException();
            if (closed) {
                request.close();
                throw new IOException(
                    "The FileChannel is already closed, " + id.getFilePath().toFile().getAbsolutePath());
            }
            pendingRequest.add(request);
            if (requetsQueue.addRequest(request, bytes)) {
                return addListener();
            } else {
                return NOT_BLOCKED;
            }
        }
    }

    private void checkIOException() {
        if (iex != null) {
            throw new RuntimeException(iex);
        }
    }

    private void notifyListener(IOException ioe) {
        SettableFuture toSetFuture = null;
        synchronized (lock) {
            if (writeFuture != null) {
                toSetFuture = writeFuture;
                writeFuture = null;
            }
        }
        if (toSetFuture != null) {
            toSetFuture.set(ioe);
        }
    }

    private void handleFinish(IORequest.WriteIORequest request, IOException e, boolean nextRound, boolean needNotify) {
        synchronized (lock) {
            boolean notified = false;
            try {
                if (iex == null && e != null) {
                    iex = e;
                    notifyListener(iex);
                    notified = true;
                }

                if (!notified && needNotify) {
                    notifyListener(null);
                    notified = true;
                }
            } finally {
                if (!nextRound || e != null || iex != null) {
                    request.close();
                    pendingRequest.remove(request);
                    if (this.pendingRequest.size() == 0) {
                        if (!notified) {
                            notifyListener(null);
                        }

                        if (closed) {
                            lock.notify();
                        }
                    }
                } else if (request != null && !request.isClosed() && nextRound && iex == null) {
                    this.requetsQueue.addRequest(request, request.getBytes());
                } else if (request.isClosed()) {
                    pendingRequest.remove(request);
                    if (this.pendingRequest.size() == 0) {
                        if (!notified) {
                            notifyListener(null);
                        }

                        if (closed) {
                            lock.notify();
                        }
                    }
                }
            }
        }
    }

    private ListenableFuture addListener() {
        ListenableFuture returnFuture = null;
        synchronized (lock) {
            checkIOException();
            checkState(writeFuture == null, "writeFuture already set");
            writeFuture = SettableFuture.create();
            returnFuture = writeFuture;
        }
        return returnFuture;
    }

    public class PageWriteRequest implements IORequest.WriteIORequest {
        private Chunk page;
        private boolean closed;
        private boolean free;

        public PageWriteRequest(Chunk page) {
            this.page = page;
        }

        @Override
        public void requestDone(IOException e, boolean needNotify) {
            handleFinish(this, e, false, needNotify);
        }

        @Override
        public synchronized long write() throws IOException {
            if (closed) {
                return 0;
            }
            long writeSizeInBytes = PagesSerdeUtil.writeChunk(serde, output, page);
            free = true;
            spillMonitor.updateBytes(writeSizeInBytes);
            return writeSizeInBytes;
        }

        @Override
        public long getBytes() {
            return this.page.estimateSize();
        }

        public SliceOutput getOutput() {
            return output;
        }

        @Override
        public synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            if (!free) {
                free = true;
            }
        }

        @Override
        public boolean isClosed() {
            return closed;
        }
    }

    private class PagesWriteRequest implements IORequest.WriteIORequest {
        private Iterator<Chunk> pageIterator;
        private ListenableFuture<?> writeFuture;
        private boolean closed;
        private final Object requestLock = new Object();

        public PagesWriteRequest(Iterator<Chunk> pageIterator) {
            this.pageIterator = pageIterator;
            // future set to done, only when all pages were writen or exception was got
            writeFuture = addListener();
        }

        public ListenableFuture getWriteFuture() {
            return writeFuture;
        }

        @Override
        public void requestDone(IOException ioe, boolean needNotify) {
            boolean nextRound = false;
            synchronized (requestLock) {
                try {
                    nextRound = pageIterator.hasNext();
                } catch (Exception e) {
                    if (ioe == null) {
                        ioe = new IOException(e);
                    }
                }
            }
            // future set to done, only when all pages were writen or exception was got
            // so set needNotify to false
            handleFinish(this, ioe, nextRound, false);
        }

        public SliceOutput getOutput() {
            return output;
        }

        @Override
        public long write() throws IOException {
            synchronized (requestLock) {
                Chunk nextPage = null;
                try {
                    if (closed || !pageIterator.hasNext()) {
                        return 0;
                    }
                    nextPage = pageIterator.next();
                    long writeSizeInBytes = PagesSerdeUtil.writeChunk(serde, output, nextPage);
                    spillMonitor.updateBytes(writeSizeInBytes);
                    return writeSizeInBytes;
                } finally {
                }
            }
        }

        @Override
        public long getBytes() {
            return -1;
        }

        @Override
        public void close() {
            synchronized (requestLock) {
                if (closed) {
                    return;
                }
                while (pageIterator.hasNext()) {
                    pageIterator.next();
                }
                closed = true;
            }
        }

        @Override
        public boolean isClosed() {
            return closed;
        }
    }
}
