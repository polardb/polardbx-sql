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

import com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GuavaFileSystemRateLimiter implements FileSystemRateLimiter {
    private static final long ACQUIRE_TIMEOUT_IN_SECOND = 5;

    private long readRate;
    private long writeRate;

    private RateLimiter readLimiter;
    private RateLimiter writeLimiter;

    private AtomicBoolean readLimiterEnabled;
    private AtomicBoolean writeLimiterEnabled;

    public GuavaFileSystemRateLimiter(long readRate, long writeRate) {
        this.readLimiterEnabled = new AtomicBoolean(readRate > 0);
        this.writeLimiterEnabled = new AtomicBoolean(writeRate > 0);
        this.readLimiter = RateLimiter.create(Math.max(readRate, 1));
        this.writeLimiter = RateLimiter.create(Math.max(writeRate, 1));
        this.readRate = readRate;
        this.writeRate = writeRate;
    }

    @Override
    public void setReadRate(double readRate) {
        this.readLimiterEnabled.set(readRate > 0);
        this.readLimiter = RateLimiter.create(Math.max(readRate, 1));
        this.readRate = (long) readRate;
    }

    @Override
    public void setWriteRate(double writeRate) {
        this.writeLimiterEnabled.set(writeRate > 0);
        this.writeLimiter = RateLimiter.create(Math.max(writeRate, 1));
        this.writeRate = (long) writeRate;
    }

    @Override
    public long getReadRate() {
        return this.readRate;
    }

    @Override
    public long getWriteRate() {
        return this.writeRate;
    }

    @Override
    public void acquireRead(int permit) throws IOException {
        if (readLimiterEnabled.get() && permit > 0) {
            boolean timeout = !readLimiter.tryAcquire(permit, ACQUIRE_TIMEOUT_IN_SECOND, TimeUnit.SECONDS);
            if (timeout) {
                throw new IOException(
                    String.format(
                        "read timeout for rate limiter: %s s with max rate: %s bytes/s",
                        ACQUIRE_TIMEOUT_IN_SECOND,
                        this.readRate)
                );
            }
        }
    }

    @Override
    public void acquireWrite(int permit) throws IOException {
        if (writeLimiterEnabled.get() && permit > 0) {
            boolean timeout = !writeLimiter.tryAcquire(permit, ACQUIRE_TIMEOUT_IN_SECOND, TimeUnit.SECONDS);
            if (timeout) {
                throw new IOException(
                    String.format(
                        "write timeout for rate limiter: %s s with max rate: %s bytes/s",
                        ACQUIRE_TIMEOUT_IN_SECOND,
                        this.writeRate)
                );
            }
        }
    }
}
