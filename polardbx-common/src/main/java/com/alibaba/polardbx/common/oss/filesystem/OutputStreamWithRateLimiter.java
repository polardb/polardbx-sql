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

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

public class OutputStreamWithRateLimiter extends OutputStream {
    final private FileSystemRateLimiter rateLimiter;
    final private OutputStream out;

    public OutputStreamWithRateLimiter(OutputStream out, FileSystemRateLimiter rateLimiter) {
        this.out = out;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public synchronized void write(byte @NotNull [] b) throws IOException {
        rateLimiter.acquireWrite(b.length);
        out.write(b);
    }

    @Override
    public synchronized void write(byte @NotNull [] b, int off, int len) throws IOException {
        rateLimiter.acquireWrite(len);
        out.write(b, off, len);
    }

    @Override
    public synchronized void flush() throws IOException {
        out.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        out.close();
    }

    @Override
    public synchronized void write(int b) throws IOException {
        rateLimiter.acquireWrite(1);
        out.write(b);
    }

    public OutputStream getWrappedStream() {
        return out;
    }
}
