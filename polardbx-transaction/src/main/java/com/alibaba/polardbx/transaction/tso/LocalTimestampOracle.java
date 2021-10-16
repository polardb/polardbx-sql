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

package com.alibaba.polardbx.transaction.tso;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;

import java.util.concurrent.atomic.AtomicLong;

public final class LocalTimestampOracle extends AbstractLifecycle implements ITimestampOracle {

    private final AtomicLong localClock;

    public LocalTimestampOracle() {
        localClock = new AtomicLong();
    }

    public LocalTimestampOracle(final long init) {
        localClock = new AtomicLong(init);
    }

    @Override
    public void setTimeout(long timeout) {
    }

    private long next(long threshold) {
        // Make sure localClock is beyond the threshold
        long last = localClock.get();
        while (last < threshold && !localClock.compareAndSet(last, threshold)) {
            last = localClock.get();
        }
        return localClock.incrementAndGet();
    }

    @Override
    public long nextTimestamp() {
        return next(System.currentTimeMillis() << BITS_LOGICAL_TIME);
    }
}
