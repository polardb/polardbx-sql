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

package com.alibaba.polardbx.common.oss.filesystem.cache;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class CacheStats {
    private final AtomicLong inMemoryRetainedBytes = new AtomicLong();
    private final AtomicLong hit = new AtomicLong();
    private final AtomicLong miss = new AtomicLong();
    private final AtomicLong quotaExceed = new AtomicLong();
    private final AtomicLong unavailable = new AtomicLong();

    public void incrementCacheHit() {
        hit.getAndIncrement();
    }

    public void incrementCacheMiss() {
        miss.getAndIncrement();
    }

    public void incrementCacheUnavailable() {
        unavailable.getAndIncrement();
    }

    public void incrementQuotaExceed() {
        quotaExceed.getAndIncrement();
    }

    public void addInMemoryRetainedBytes(long bytes) {
        inMemoryRetainedBytes.addAndGet(bytes);
    }

    public long getInMemoryRetainedBytes() {
        return inMemoryRetainedBytes.get();
    }

    public long getCacheHit() {
        return hit.get();
    }

    public long getCacheMiss() {
        return miss.get();
    }

    public long getQuotaExceed() {
        return quotaExceed.get();
    }

    public long getCacheUnavailable() {
        return unavailable.get();
    }

    public void reset() {
        inMemoryRetainedBytes.set(0);
        hit.set(0);
        miss.set(0);
        quotaExceed.set(0);
        unavailable.set(0);
    }

    @Override
    public String toString() {
        return "CacheStats{" +
            "inMemoryRetainedBytes=" + inMemoryRetainedBytes +
            ", hit=" + hit +
            ", miss=" + miss +
            ", quotaExceed=" + quotaExceed +
            ", unavailable=" + unavailable +
            '}';
    }
}