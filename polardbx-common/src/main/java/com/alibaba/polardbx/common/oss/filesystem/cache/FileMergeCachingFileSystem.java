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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

import static java.util.Objects.requireNonNull;

public final class FileMergeCachingFileSystem
    extends CachingFileSystem {
    private final CacheManager cacheManager;
    private final boolean cacheValidationEnabled;
    private final boolean enableCache;

    public FileMergeCachingFileSystem(
        URI uri,
        Configuration configuration,
        CacheManager cacheManager,
        FileSystem dataTier,
        boolean cacheValidationEnabled,
        boolean enableCache) {
        super(dataTier, uri);
        requireNonNull(configuration, "configuration is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.cacheValidationEnabled = cacheValidationEnabled;

        setConf(configuration);

        this.enableCache = enableCache;
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
        return new FileMergeCachingInputStream(
            dataTier.open(path),
            cacheManager,
            path,
            enableCache ? CacheQuota.NO_CACHE_CONSTRAINTS : CacheQuota.DISABLE_CACHE,
            cacheValidationEnabled);
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public void close() throws IOException {
        cacheManager.close();
        super.close();
    }
}
