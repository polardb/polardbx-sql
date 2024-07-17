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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerde;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeUtil;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class AsyncPageFileChannelReader {
    private static final Logger log = LoggerFactory.getLogger(AsyncPageFileChannelReader.class);
    private final FileHolder id;
    private final PagesSerde serde;
    private final ReadQueue requetsQueue;

    private final Object lock = new Object();
    private final AtomicLong pendingBytes = new AtomicLong(0);
    private final AtomicLong pendingRequest = new AtomicLong(0);

    @GuardedBy("lock")
    private IOException iex;
    @GuardedBy("lock")
    private boolean closed;

    private SliceInput input;
    private Runnable onClose;
    private final ArrayList<PagesIteratorReadRequest> blockedRequest = new ArrayList<>();
    private PagesListReadRequest pagesListReadRequest = null;
    private SpillMonitor spillMonitor;

    public AsyncPageFileChannelReader(FileHolder id, PagesSerde serde, ReadQueue requetsQueue,
                                      Runnable noThrowableOnClose, SpillMonitor spillMonitor)
        throws IOException {
        this.id = requireNonNull(id, "FileId is null");
        this.serde = requireNonNull(serde, "PagesSerde is null");
        this.requetsQueue = requireNonNull(requetsQueue, "runningRequests is null");
        this.input = new InputStreamSliceInput(new BufferedInputStream(new FileInputStream(id.getFilePath().toFile())));
        this.onClose = requireNonNull(noThrowableOnClose);
        this.spillMonitor = spillMonitor;
    }

    public void close()
        throws IOException {
        synchronized (lock) {
            if (closed) {
                return;
            }
            try {
                closed = true;
                checkIOException();
                while (pendingRequest.get() > 0) {
                    try {
                        if (blockedRequest.size() > 0) {
                            ImmutableList<PagesIteratorReadRequest> list = ImmutableList.copyOf(blockedRequest);
                            for (PagesIteratorReadRequest readRequest : list) {
                                readRequest.close();
                                blockedRequest.remove(readRequest);
                            }
                        }
                        this.lock.wait(100);
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                    checkIOException();
                }
            } finally {
                long bytes = pendingBytes.get();
                if (bytes > 0) {
                    // clear pending bytes
                    requetsQueue.onReady(-bytes);
                    pendingBytes.set(0L);
                }
                if (pagesListReadRequest != null) {
                    pagesListReadRequest.close();
                }
                this.onClose.run();
                if (input != null) {
                    input.close();
                    input = null;
                }
            }
        }
    }

    public void closeAndDelete()
        throws IOException {
        this.close();
        if (id != null && id.getFilePath() != null) {
            id.getFilePath().toFile().delete();
            if (id.getFilePath().toFile().exists()) {
                throw new IOException("fail to delete file " + id.getFilePath().toFile().getAbsolutePath());
            }
        }
    }

    public Iterator<Chunk> readPagesIterator()
        throws IOException {
        PagesIteratorReadRequest request = new PagesIteratorReadRequest();
        addRequest(request);
        return request.getIterator();
    }

    /**
     * 最多读取maxChunkNum个Chunk
     */
    public Iterator<Chunk> readPagesIterator(long maxChunkNum)
        throws IOException {
        PagesIteratorReadRequest request = new PagesIteratorReadRequest(maxChunkNum);
        addRequest(request);
        return request.getIterator();
    }

    public ListenableFuture<List<Chunk>> readPagesList()
        throws IOException {
        checkState(pagesListReadRequest == null, "pagesListReadRequest not null");
        this.pagesListReadRequest = new PagesListReadRequest();
        addRequest(this.pagesListReadRequest);
        return this.pagesListReadRequest.getReadFuture();
    }

    public void addRequest(IORequest.ReadIORequest request)
        throws IOException {
        synchronized (lock) {
            checkIOException();
            pendingRequest.incrementAndGet();
            if (closed) {
                pendingRequest.decrementAndGet();
                throw new IOException(
                    "The FileChannel is already closed, " + id.getFilePath().toFile().getAbsolutePath());
            }
            requetsQueue.addRequest(request);
        }
    }

    private void checkIOException() {
        if (iex != null) {
            throw new RuntimeException(iex);
        }
    }

    private void handleFinish(IOException e, Chunk page, ReadCallback callback, boolean nextRound, long pageBytes,
                              boolean forceFinish) {
        boolean pending = true;
        boolean onFailure = false;
        synchronized (lock) {
            // record io exception
            if (iex == null && e != null) {
                iex = e;
            }
            try {
                if (iex == null) {
                    if (pageBytes > 0) {
                        pending = requetsQueue.onReady(pageBytes);
                        pendingBytes.addAndGet(pageBytes);
                    }
                    callback.onPageReady(page);
                } else if (e != null) {
                    callback.onFailure(e);
                    onFailure = true;
                }
                // check is finish
                if (!nextRound && iex == null) {
                    callback.onSuccessful();
                }
            } catch (Throwable t) {
                // record unkown exception and decrese reqest in finally block
                iex = new IOException(t);
                if (!onFailure) {
                    callback.onFailure(iex);
                    onFailure = true;
                }
            } finally {
                if (iex != null || e != null || !nextRound || forceFinish) {
                    if (pendingRequest.decrementAndGet() == 0 && closed) {
                        lock.notify();
                    }
                } else if (nextRound) {
                    callback.requeue(pending);
                }
            }
        }
    }

    private void handleBlock(PagesIteratorReadRequest pagesIteratorReadRequest) {
        synchronized (lock) {
            blockedRequest.add(pagesIteratorReadRequest);
        }
    }

    public interface ReadCallback {
        void onSuccessful();

        void onPageReady(Chunk page);

        void onFailure(IOException ioe);

        void requeue(boolean pending);
    }

    private class PagesIteratorReadRequest
        implements IORequest.ReadIORequest, ReadCallback {
        private final Object reqLock = new Object();

        @GuardedBy("reqLock")
        private final List<Chunk> pageBuffers = new LinkedList<>();

        @GuardedBy("reqLock")
        private boolean finish;

        @GuardedBy("reqLock")
        private boolean blocked;

        private Iterator<Chunk> pageIterator;

        private Chunk page;
        private boolean closed;

        //最多读取maxChunkNum个Chunk；0代表没有限制
        private long maxChunkNum = 0;
        //已从input中读取的数目
        private long readChunks = 0;

        public PagesIteratorReadRequest() {

        }

        public PagesIteratorReadRequest(long maxChunkNum) {
            this.maxChunkNum = maxChunkNum;
        }

        @Override
        public void requestDone(IOException e) {
            boolean nextRound = false;
            if (e == null) {
                try {
                    //已读取的readChunks超过maxChunkNum时，nextRound=false，不再读取下一个
                    if (maxChunkNum <= 0 || readChunks < maxChunkNum) {
                        nextRound = pageIterator.hasNext();
                    }
                } catch (Exception ioe) {
                    e = new IOException(ioe);
                }
            }

            handleFinish(e, page, this, nextRound, page != null ? page.getElementUsedBytes() : 0L, false);
        }

        @Override
        public long read()
            throws IOException {
            if (pageIterator == null) {
                pageIterator = PagesSerdeUtil.readPages(serde, input);
            }
            page = null;
            if (pageIterator.hasNext()) {
                page = pageIterator.next();
                readChunks++;
                return page.getElementUsedBytes();
            }
            return 0;
        }

        public Iterator<Chunk> getIterator() {
            return new AbstractIterator<Chunk>() {
                @Override
                protected Chunk computeNext() {
                    synchronized (reqLock) {
                        // break when get Exception or read is finish and pageBuffers is empty
                        while (!(finish && iex != null || finish && pageBuffers.isEmpty()) && !closed) {
                            checkIOException();

                            if (!pageBuffers.isEmpty()) {
                                Chunk retPage = pageBuffers.remove(0);
                                long pageBytes = retPage.getElementUsedBytes();
                                AsyncPageFileChannelReader.this.pendingBytes.addAndGet(-pageBytes);
                                boolean pending = requetsQueue.onRead(pageBytes);
                                if (blocked && (!pending || pageBuffers.isEmpty())) {
                                    blockedRequest.remove(PagesIteratorReadRequest.this);
                                    requetsQueue.addRequest(PagesIteratorReadRequest.this);
                                    blocked = false;
                                }
                                return retPage;
                            } else {
                                try {
                                    reqLock.wait();
                                } catch (InterruptedException e) {
                                }
                            }
                        }

                        checkIOException();
                        return endOfData();
                    }
                }
            };
        }

        //ReadCallback
        @Override
        public void onSuccessful() {
            synchronized (reqLock) {
                finish = true;
                reqLock.notify();
            }
        }

        @Override
        public void onPageReady(Chunk page) {
            synchronized (reqLock) {
                if (page != null) {
                    pageBuffers.add(page);
                    if (pageBuffers.size() == 1) {
                        reqLock.notify();
                    }
                }
            }
        }

        @Override
        public void onFailure(IOException ioe) {
            synchronized (reqLock) {
                finish = true;
                reqLock.notify();
            }
        }

        @Override
        public void requeue(boolean pending) {
            synchronized (reqLock) {
                if (!pending || pageBuffers.isEmpty()) {
                    requetsQueue.addRequest(PagesIteratorReadRequest.this);
                } else {
                    handleBlock(this);
                    blocked = true;
                }
            }
        }

        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            handleFinish(null, null, this, false, 0, true);
        }
    }

    private class PagesListReadRequest
        implements IORequest.ReadIORequest, ReadCallback {
        private final List<Chunk> pageBuffers = new LinkedList<>();

        private SettableFuture<List<Chunk>> future;

        private Iterator<Chunk> pageIterator;

        private Chunk page;

        private boolean closed;

        public PagesListReadRequest() {
            future = SettableFuture.create();
        }

        public ListenableFuture<List<Chunk>> getReadFuture() {
            return future;
        }

        @Override
        public void requestDone(IOException e) {
            boolean nextRound = false;
            if (e == null) {
                try {
                    nextRound = pageIterator.hasNext();
                } catch (Exception ioe) {
                    e = new IOException(ioe);
                }
            }

            // pageBytes == 0, will not pending next read operation
            handleFinish(e, page, this, nextRound, 0L, false);
        }

        @Override
        public long read()
            throws IOException {
            if (pageIterator == null) {
                pageIterator = PagesSerdeUtil.readPages(serde, new InputStreamSliceInput(input));
            }
            page = null;
            if (pageIterator.hasNext()) {
                page = pageIterator.next();
                return page.getElementUsedBytes();
            }
            return 0;
        }

        //ReadCallback
        @Override
        public void onSuccessful() {
            this.future.set(pageBuffers);
        }

        @Override
        public void onPageReady(Chunk page) {
            if (page != null && !closed) {
                pageBuffers.add(page);
            }
        }

        @Override
        public void onFailure(IOException ioe) {
            this.future.setException(ioe);
        }

        @Override
        public void requeue(boolean pending) {
            requetsQueue.addRequest(PagesListReadRequest.this);
        }

        public void close() {
            if (closed) {
                return;
            }
            closed = true;
        }
    }
}
