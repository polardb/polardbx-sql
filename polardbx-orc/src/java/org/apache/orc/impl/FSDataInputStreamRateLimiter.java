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

package org.apache.orc.impl;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

public class FSDataInputStreamRateLimiter extends FSDataInputStream {

    public FSDataInputStreamRateLimiter(FSDataInputStream in, Consumer<Integer> rateLimiter) throws IOException {
        super(new InputStreamRateLimiter(in.getWrappedStream(), rateLimiter));
    }

    private static class InputStreamRateLimiter extends InputStream implements Seekable, PositionedReadable {
        private final Consumer<Integer> rateLimiter;
        private final InputStream wrappedStream;

        public InputStreamRateLimiter(InputStream inputStream, Consumer<Integer> rateLimiter) {
            this.wrappedStream = inputStream;
            this.rateLimiter = rateLimiter;
        }

        private void acquire(int length) {
            if (rateLimiter != null) {
                rateLimiter.accept(length);
            }
        }

        @Override
        public int read() throws IOException {
            acquire(1);
            return wrappedStream.read();
        }

        @Override
        public int read(byte @NotNull [] b) throws IOException {
            acquire(b.length);
            return wrappedStream.read(b);
        }

        @Override
        public int read(byte @NotNull [] b, int off, int len) throws IOException {
            acquire(len);
            return wrappedStream.read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return wrappedStream.skip(n);
        }

        @Override
        public int available() throws IOException {
            return wrappedStream.available();
        }

        @Override
        public void close() throws IOException {
            wrappedStream.close();
        }

        @Override
        public synchronized void mark(int readLimit) {
            wrappedStream.mark(readLimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            wrappedStream.reset();
        }

        @Override
        public boolean markSupported() {
            return wrappedStream.markSupported();
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            acquire(length);
            return ((PositionedReadable) wrappedStream).read(position, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            acquire(length);
            ((PositionedReadable) wrappedStream).readFully(position, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            acquire(buffer.length);
            ((PositionedReadable) wrappedStream).readFully(position, buffer);
        }

        @Override
        public void seek(long l) throws IOException {
            ((Seekable) wrappedStream).seek(l);
        }

        @Override
        public long getPos() throws IOException {
            return ((Seekable) wrappedStream).getPos();
        }

        @Override
        public boolean seekToNewSource(long l) throws IOException {
            return ((Seekable) wrappedStream).seekToNewSource(l);
        }
    }

}
