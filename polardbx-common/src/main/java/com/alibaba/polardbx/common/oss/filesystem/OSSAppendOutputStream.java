/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.oss.filesystem;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Syncable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

/**
 * Append Object for Oss. Support synchronous and asynchronous.
 */
public class OSSAppendOutputStream extends OutputStream implements Syncable {
    private static final Logger LOG = LoggerFactory.getLogger(OSSAppendOutputStream.class);
    private final OSSFileSystemStore store;
    private final Configuration conf;
    private final String key;
    private final ListeningExecutorService executorService;
    private final FileSystemRateLimiter rateLimiter;

    private volatile boolean closed = false;
    private final byte[] singleByte = new byte[1];
    private final int bufferSize;
    private File localFile;
    private long localFileId = 0L;
    private OutputStream bufferStream;
    private long writeLength = 0L;
    private long ossFilePosition;
    private ListenableFuture<Long> asyncJob = null;
    private File currentUploadOssFile = null;
    private final List<File> historyTempFiles = new ArrayList<>();

    public OSSAppendOutputStream(Configuration conf,
                                 OSSFileSystemStore store,
                                 String key,
                                 int bufferSize,
                                 ExecutorService executorService,
                                 FileSystemRateLimiter rateLimiter) throws IOException {
        this.conf = conf;
        this.store = store;
        this.key = key;
        this.bufferSize = bufferSize;
        this.executorService = MoreExecutors.listeningDecorator(executorService);
        this.rateLimiter = rateLimiter;
        this.localFile = createTmpLocalFile();
        this.bufferStream = new BufferedOutputStream(new FileOutputStream(localFile), bufferSize);
        this.ossFilePosition = store.getOssFileLength(key);
        historyTempFiles.add(localFile);
    }

    @Override
    public synchronized void write(int b) throws IOException {
        singleByte[0] = (byte) b;
        write(singleByte, 0, 1);
    }

    @Override
    public synchronized void write(byte @NotNull [] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream closed.");
        }

        rateLimiter.acquireWrite(len);

        bufferStream.write(b, off, len);
        writeLength += len;
    }

    /**
     * Upload to Oss by sync
     *
     * @throws IOException failed
     */
    @Override
    public synchronized void flush() throws IOException {
        if (closed) {
            throw new IOException("Stream closed.");
        }
        if (asyncJob != null) {
            waitAsyncJob();
        }
        if (writeLength == 0L) {
            return;
        }

        uploadBufferToOss(false);

    }

    /**
     * upload to Oss by async. There is only one async task at a time.
     *
     * @throws IOException failed
     */
    public synchronized void asyncFlush() throws IOException {
        if (closed) {
            throw new IOException("Stream closed.");
        }

        if (asyncJob != null) {
            waitAsyncJob();
        }

        if (writeLength == 0L) {
            return;
        }

        uploadBufferToOss(true);
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        flush();

        removeTemporaryFiles();
        closed = true;
    }

    @Override
    public void hflush() throws IOException {
        flush();
    }

    @Override
    public void hsync() throws IOException {
        asyncFlush();
    }

    private File createTmpLocalFile() throws IOException {
        localFileId++;
        return OSSUtils.createTmpFileForWrite(String.format("local-%s-%d-", key, localFileId), -1, conf);
    }

    private void waitAsyncJob() throws IOException {
        try {
            ossFilePosition = asyncJob.get();
        } catch (InterruptedException ie) {
            LOG.warn(String.format("Oss append File: %s, local file: %s, Position: %d, Interrupted: ", key,
                currentUploadOssFile.getAbsolutePath(), ossFilePosition), ie);
            Thread.currentThread().interrupt();
        } catch (ExecutionException ee) {
            LOG.error(String.format("Oss append File: %s, local file: %s, Position: %d, Error: ", key,
                currentUploadOssFile.getAbsolutePath(), ossFilePosition), ee);
            throw new IOException(
                String.format("Oss append File: %s, local file: %s, Position: %d, Error: ", key,
                    currentUploadOssFile.getAbsolutePath(), ossFilePosition), ee);
        } finally {
            asyncJob = null;
        }
    }

    private void uploadBufferToOss(boolean async) throws IOException {
        bufferStream.flush();
        bufferStream.close();
        currentUploadOssFile = localFile;

        if (async) {
            asyncJob = executorService.submit(() -> store.appendObject(key, currentUploadOssFile, ossFilePosition));
        } else {
            ossFilePosition = store.appendObject(key, currentUploadOssFile, ossFilePosition);
        }

        localFile = createTmpLocalFile();
        this.bufferStream = new BufferedOutputStream(new FileOutputStream(localFile), bufferSize);
        historyTempFiles.add(localFile);
        writeLength = 0L;
    }

    private void removeTemporaryFiles() {
        for (File file : historyTempFiles) {
            if (file != null && file.exists() && !file.delete()) {
                LOG.warn("Failed to delete temporary file {}", file);
            }
        }
    }

    public long getOssFilePosition() {
        return ossFilePosition;
    }
}
