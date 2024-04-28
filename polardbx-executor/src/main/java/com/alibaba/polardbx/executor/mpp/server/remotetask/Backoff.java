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
package com.alibaba.polardbx.executor.mpp.server.remotetask;

import com.alibaba.polardbx.executor.mpp.util.MillTicker;
import com.google.common.base.Ticker;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class Backoff {
    private final long minFailureIntervalNanos;
    private final long maxFailureIntervalNanos;
    private final Ticker ticker;
    private final long[] backoffDelayIntervalsMill;
    private final long createTime;

    private long lastSuccessTime;
    private long firstRequestAfterSuccessTime;
    private long lastFailureTime;
    private long failureCount;

    public Backoff(long minFailureInterval, long maxFailureInterval) {
        this(minFailureInterval,
            maxFailureInterval,
            MillTicker.systemTicker(),
            0,
            50,
            100,
            200,
            500);
    }

    public Backoff(long minFailureInterval, long maxFailureInterval, Ticker ticker, long... backoffDelayIntervals) {
        requireNonNull(minFailureInterval, "minFailureInterval is null");
        requireNonNull(maxFailureInterval, "maxFailureInterval is null");
        requireNonNull(backoffDelayIntervals, "backoffDelayIntervals is null");
        checkArgument(backoffDelayIntervals.length > 0, "backoffDelayIntervals must contain at least one entry");
        checkArgument(maxFailureInterval >= minFailureInterval, "maxFailureInterval is less than minFailureInterval");

        this.minFailureIntervalNanos = minFailureInterval;
        this.maxFailureIntervalNanos = maxFailureInterval;
        this.ticker = ticker;
        this.backoffDelayIntervalsMill = backoffDelayIntervals;
        this.lastSuccessTime = this.ticker.read();
        this.firstRequestAfterSuccessTime = Long.MIN_VALUE;
        this.createTime = this.ticker.read();
    }

    public synchronized long getFailureCount() {
        return failureCount;
    }

    public synchronized Duration getTimeSinceLastSuccess() {
        long lastSuccessfulRequest = this.lastSuccessTime;
        long value = ticker.read() - lastSuccessfulRequest;
        return new Duration(value, MILLISECONDS).convertToMostSuccinctTimeUnit();
    }

    public synchronized void startRequest() {
        if (firstRequestAfterSuccessTime < lastSuccessTime) {
            firstRequestAfterSuccessTime = ticker.read();
        }
    }

    public synchronized void success() {
        lastSuccessTime = ticker.read();
        failureCount = 0;
        lastFailureTime = 0;
    }

    /**
     * @return true if the failure is considered permanent
     */
    public synchronized boolean failure() {
        long lastSuccessfulRequest = this.lastSuccessTime;
        long now = ticker.read();
        lastFailureTime = now;

        failureCount++;

        long failureInterval;
        if (lastSuccessfulRequest - createTime > maxFailureIntervalNanos) {
            failureInterval = maxFailureIntervalNanos;
        } else {
            failureInterval = Math.max(lastSuccessfulRequest - createTime, minFailureIntervalNanos);
        }
        long failureDuration;
        if (firstRequestAfterSuccessTime < lastSuccessTime) {
            // If user didn't call startRequest(), use the time of the last success
            failureDuration = now - lastSuccessfulRequest;
        } else {
            // Otherwise only count the time since the first request that started failing
            failureDuration = now - firstRequestAfterSuccessTime;
        }
        return failureDuration >= failureInterval;
    }

    public synchronized long getBackoffDelayMills() {
        int failureCount = (int) Math.min(backoffDelayIntervalsMill.length, this.failureCount);
        if (failureCount == 0) {
            return 0;
        }
        // expected amount of time to delay from the last failure time
        long currentDelay = backoffDelayIntervalsMill[failureCount - 1];

        // calculate expected delay from now
        long nanosSinceLastFailure = ticker.read() - lastFailureTime;
        return Math.max(0, currentDelay - nanosSinceLastFailure);
    }
}
