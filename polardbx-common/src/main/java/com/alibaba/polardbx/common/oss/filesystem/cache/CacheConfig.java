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

import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;

import javax.annotation.Nullable;
import java.net.URI;

import static com.alibaba.polardbx.common.oss.filesystem.cache.CacheQuotaScope.GLOBAL;

public class CacheConfig {
    @Deprecated
    private boolean cachingEnabled;
    @Deprecated
    private CacheType cacheType;
    @Deprecated
    private CacheQuotaScope cacheQuotaScope = GLOBAL;

    private URI baseDirectory;
    private boolean validationEnabled;
    private int flushCacheThreadNum = ThreadCpuStatUtil.NUM_CORES;

    @Nullable
    public URI getBaseDirectory() {
        return baseDirectory;
    }

    public CacheConfig setBaseDirectory(URI dataURI) {
        this.baseDirectory = dataURI;
        return this;
    }

    public boolean isValidationEnabled() {
        return validationEnabled;
    }

    public CacheConfig setValidationEnabled(boolean validationEnabled) {
        this.validationEnabled = validationEnabled;
        return this;
    }

    public CacheConfig setCachingEnabled(boolean cachingEnabled) {
        this.cachingEnabled = cachingEnabled;
        return this;
    }

    public boolean isCachingEnabled() {
        return cachingEnabled;
    }

    public CacheConfig setCacheType(CacheType cacheType) {
        this.cacheType = cacheType;
        return this;
    }

    public CacheType getCacheType() {
        return cacheType;
    }

    public CacheQuotaScope getCacheQuotaScope() {
        return cacheQuotaScope;
    }

    public CacheConfig setCacheQuotaScope(CacheQuotaScope cacheQuotaScope) {
        this.cacheQuotaScope = cacheQuotaScope;
        return this;
    }

    public int getFlushCacheThreadNum() {
        return flushCacheThreadNum;
    }

    public void setFlushCacheThreadNum(int flushCacheThreadNum) {
        this.flushCacheThreadNum = flushCacheThreadNum;
    }
}
