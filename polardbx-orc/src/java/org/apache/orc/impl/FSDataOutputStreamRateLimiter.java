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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;

public class FSDataOutputStreamRateLimiter extends FSDataOutputStream {

    public FSDataOutputStreamRateLimiter(FSDataOutputStream out, Consumer<Integer> rateLimiter) throws IOException {
        super(new OutputStreamRateLimiter(out.getWrappedStream(), rateLimiter), null, out.getPos());
    }

    private static class OutputStreamRateLimiter extends OutputStream {

        private final Consumer<Integer> rateLimiter;
        private final OutputStream wrappedStream;

        public OutputStreamRateLimiter(OutputStream out, Consumer<Integer> rateLimiter) {
            this.wrappedStream = out;
            this.rateLimiter = rateLimiter;
        }

        private void acquire(int length) {
            if (rateLimiter != null) {
                rateLimiter.accept(length);
            }
        }

        @Override
        public void write(int b) throws IOException {
            acquire(1);
            wrappedStream.write(b);
        }

        @Override
        public void write(byte @NotNull [] b, int off, int len) throws IOException {
            acquire(len);
            wrappedStream.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            wrappedStream.flush();
        }

        @Override
        public void close() throws IOException {
            wrappedStream.close();
        }

    }

}
