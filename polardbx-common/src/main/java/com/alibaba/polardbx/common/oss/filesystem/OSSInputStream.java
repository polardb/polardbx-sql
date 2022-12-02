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

package com.alibaba.polardbx.common.oss.filesystem;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;

import static com.alibaba.polardbx.common.oss.filesystem.Constants.MULTIPART_DOWNLOAD_SIZE_DEFAULT;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.MULTIPART_DOWNLOAD_SIZE_KEY;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.OSS_FETCH_POLICY;

/**
 * The input stream for OSS blob system.
 * The class uses multi-part downloading to read data from the object content
 * stream.
 */
public class OSSInputStream extends FSInputStream {
    public static final Logger LOG = LoggerFactory.getLogger(OSSInputStream.class);
    private final long downloadPartSize;
    private OSSFileSystemStore store;
    private final String key;
    private FileSystem.Statistics statistics;
    private boolean closed;
    private long contentLength;
    private long position;
    private long partRemaining;
    private byte[] buffer;
    private int maxReadAheadPartNumber;
    private long expectNextPos;
    private long lastByteStart;

    private ExecutorService readAheadExecutorService;
    private Queue<ReadBuffer> readBufferQueue = new ArrayDeque<>();

    private final FetchPolicy fetchPolicy;

    private FileSystemRateLimiter rateLimiter;

    public OSSInputStream(Configuration conf,
                          ExecutorService readAheadExecutorService, int maxReadAheadPartNumber,
                          OSSFileSystemStore store, String key, Long contentLength,
                          FileSystem.Statistics statistics, FileSystemRateLimiter rateLimiter) throws IOException {
        this.readAheadExecutorService =
            MoreExecutors.listeningDecorator(readAheadExecutorService);
        this.store = store;
        this.key = key;
        this.statistics = statistics;
        this.contentLength = contentLength;
        downloadPartSize = conf.getLong(MULTIPART_DOWNLOAD_SIZE_KEY,
            MULTIPART_DOWNLOAD_SIZE_DEFAULT);
        this.maxReadAheadPartNumber = maxReadAheadPartNumber;

        this.expectNextPos = 0;
        this.lastByteStart = -1;

        // don't prefetch here.
        // reopen(0);

        this.closed = false;
        this.fetchPolicy = FetchPolicy.valueOf(conf.get(OSS_FETCH_POLICY, FetchPolicy.REQUESTED.name()));
        this.rateLimiter = rateLimiter;
    }

    private synchronized void reopen(long pos, long downloadPartSize) throws IOException {
        long partSize;

        if (pos < 0) {
            throw new EOFException("Cannot seek at negative position:" + pos);
        } else if (pos > contentLength) {
            throw new EOFException("Cannot seek after EOF, contentLength:" +
                contentLength + " position:" + pos);
        } else if (pos + downloadPartSize > contentLength) {
            // current pos + download size, exceed the file size.
            partSize = contentLength - pos;
        } else {
            partSize = downloadPartSize;
        }

        if (this.buffer != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Aborting old stream to open at pos " + pos);
            }
            this.buffer = null;
        }

        boolean isRandomIO = true;
        if (pos == this.expectNextPos) {
            isRandomIO = false;
        } else {
            //new seek, remove cache buffers if its byteStart is not equal to pos
            while (readBufferQueue.size() != 0) {
                if (readBufferQueue.element().getByteStart() != pos) {
                    readBufferQueue.poll();
                } else {
                    break;
                }
            }
        }

        this.expectNextPos = pos + partSize;

        int currentSize = readBufferQueue.size();
        if (currentSize == 0) {
            //init lastByteStart to pos - partSize, used by for loop below
            lastByteStart = pos - partSize;
        } else {
            ReadBuffer[] readBuffers = readBufferQueue.toArray(
                new ReadBuffer[currentSize]);
            lastByteStart = readBuffers[currentSize - 1].getByteStart();
        }

        // current size: current read ahead part number
        int maxLen = this.maxReadAheadPartNumber - currentSize;
        for (int i = 0; i < maxLen && i < (currentSize + 1) * 2; i++) {
            // 处理跨页
            if (lastByteStart + partSize * (i + 1) > contentLength) {
                break;
            }

            long byteStart = lastByteStart + partSize * (i + 1);
            long byteEnd = byteStart + partSize - 1;
            if (byteEnd >= contentLength) {
                byteEnd = contentLength - 1;
            }

            ReadBuffer readBuffer = new ReadBuffer(byteStart, byteEnd);
            if (readBuffer.getBuffer().length == 0) {
                //EOF
                readBuffer.setStatus(ReadBuffer.STATUS.SUCCESS);
            } else {
                this.readAheadExecutorService.execute(
                    new OSSFileReaderTask(key, store, readBuffer));
            }
            readBufferQueue.add(readBuffer);
            if (isRandomIO) {
                break;
            }
        }

        ReadBuffer readBuffer = readBufferQueue.poll();
        readBuffer.lock();
        try {
            readBuffer.await(ReadBuffer.STATUS.INIT);
            if (readBuffer.getStatus() == ReadBuffer.STATUS.ERROR) {
                this.buffer = null;
            } else {
                this.buffer = readBuffer.getBuffer();
            }
        } catch (InterruptedException e) {
            LOG.warn("interrupted when wait a read buffer");
        } finally {
            readBuffer.unlock();
        }

        if (this.buffer == null) {
            throw new IOException("Null IO stream");
        }
        position = pos;
        partRemaining = partSize;
    }

    /**
     * Reopen the wrapped stream at give position, by seeking for
     * data of a part length from object content stream.
     *
     * @param pos position from start of a file
     * @throws IOException if failed to reopen
     */
    private synchronized void reopen(long pos) throws IOException {
        reopen(pos, this.downloadPartSize);
    }

    @Override
    public synchronized int read() throws IOException {
        checkNotClosed();

        if (partRemaining <= 0 && position < contentLength) {
            reopen(position);
        }

        int byteRead = -1;
        if (partRemaining != 0) {
            byteRead = this.buffer[this.buffer.length - (int) partRemaining] & 0xFF;
        }
        if (byteRead >= 0) {
            position++;
            partRemaining--;
        }

        if (statistics != null && byteRead >= 0) {
            statistics.incrementBytesRead(byteRead);
        }
        return byteRead;
    }

    /**
     * Verify that the input stream is open. Non blocking; this gives
     * the last state of the volatile {@link #closed} field.
     *
     * @throws IOException if the connection is closed.
     */
    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    private static final int sizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return n + 1;
    }

    @Override
    public synchronized int read(byte[] buf, int off, int len)
        throws IOException {
        checkNotClosed();

        if (buf == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > buf.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        rateLimiter.acquireRead(len);

        int bytesRead = 0;
        // Not EOF, and read not done
        while (position < contentLength && bytesRead < len) {
            if (partRemaining == 0) {
                int delta = len - bytesRead;

                switch (fetchPolicy) {
                case FIXED:
                    reopen(position);
                    break;
                case MERGE:
                    if (delta > this.downloadPartSize) {
                        int toRead = sizeFor(delta);

                        while (toRead - delta > this.downloadPartSize) {
                            toRead = sizeFor(delta -= this.downloadPartSize);
                        }
                        if (toRead < this.downloadPartSize) {
                            toRead = (int) this.downloadPartSize;
                        }

                        reopen(position, toRead);
                    } else {
                        reopen(position);
                    }
                    break;
                case REQUESTED:
                    reopen(position, delta);
                    break;
                }
            }

            int bytes = 0;
            for (int i = this.buffer.length - (int) partRemaining;
                 i < this.buffer.length; i++) {
                buf[off + bytesRead] = this.buffer[i];
                bytes++;
                bytesRead++;
                if (off + bytesRead >= len) {
                    break;
                }
            }

            if (bytes > 0) {
                position += bytes;
                partRemaining -= bytes;
            } else if (partRemaining != 0) {
                throw new IOException("Failed to read from stream. Remaining:" +
                    partRemaining);
            }
        }

        if (statistics != null && bytesRead > 0) {
            statistics.incrementBytesRead(bytesRead);
        }

        // Read nothing, but attempt to read something
        if (bytesRead == 0 && len > 0) {
            return -1;
        } else {
            return bytesRead;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        this.buffer = null;
    }

    @Override
    public synchronized int available() throws IOException {
        checkNotClosed();

        long remaining = contentLength - position;
        if (remaining > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) remaining;
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
        checkNotClosed();

        position = pos;
        partRemaining = 0;
    }

    @Override
    public synchronized long getPos() throws IOException {
        checkNotClosed();
        return position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        checkNotClosed();
        return false;
    }

    public long getExpectNextPos() {
        return this.expectNextPos;
    }
}

