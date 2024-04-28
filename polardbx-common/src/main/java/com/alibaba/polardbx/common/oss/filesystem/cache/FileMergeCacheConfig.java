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

import io.airlift.slice.DataSize;
import io.airlift.slice.Duration;

import static io.airlift.slice.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;

public class FileMergeCacheConfig {
    private boolean enableCache = true;
    private int maxCachedEntries = 2048;
    private Duration cacheTtl = new Duration(2, DAYS);
    private DataSize maxInMemoryCacheSize = new DataSize(2, GIGABYTE);
    private DataSize maxInDiskCacheSize = new DataSize(100, GIGABYTE);

    /**
     * To use bytes cache in Local File Cache.
     */
    private boolean useByteCache = false;
    private double memoryRatioOfBytesCache = 0.3d;

    public int getMaxCachedEntries() {
        return maxCachedEntries;
    }

    public FileMergeCacheConfig setMaxCachedEntries(int maxCachedEntries) {
        this.maxCachedEntries = maxCachedEntries;
        return this;
    }

    public double getMemoryRatioOfBytesCache() {
        return memoryRatioOfBytesCache;
    }

    public FileMergeCacheConfig setMemoryRatioOfBytesCache(double memoryRatioOfBytesCache) {
        this.memoryRatioOfBytesCache = memoryRatioOfBytesCache;
        return this;
    }

    public boolean isUseByteCache() {
        return useByteCache;
    }

    public FileMergeCacheConfig setUseByteCache(boolean useByteCache) {
        this.useByteCache = useByteCache;
        return this;
    }

    public DataSize getMaxInMemoryCacheSize() {
        return maxInMemoryCacheSize;
    }

    public FileMergeCacheConfig setMaxInMemoryCacheSize(DataSize maxInMemoryCacheSize) {
        this.maxInMemoryCacheSize = maxInMemoryCacheSize;
        return this;
    }

    public Duration getCacheTtl() {
        return cacheTtl;
    }

    public FileMergeCacheConfig setCacheTtl(Duration cacheTtl) {
        this.cacheTtl = cacheTtl;
        return this;
    }

    public DataSize getMaxInDiskCacheSize() {
        return maxInDiskCacheSize;
    }

    public void setMaxInDiskCacheSize(DataSize maxInDiskCacheSize) {
        this.maxInDiskCacheSize = maxInDiskCacheSize;
    }

    public boolean isEnableCache() {
        return enableCache;
    }

    public void setEnableCache(boolean enableCache) {
        this.enableCache = enableCache;
    }
}