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

package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.oss.filesystem.FileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.GuavaFileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.NFSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheType;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;

public class NFSInstanceInitializer {

    /**
     * nfs://server<:port>/path
     */

    public String uri;
    public CachePolicy cachePolicy;

    public NFSInstanceInitializer() {

    }

    public static NFSInstanceInitializer newBuilder() {
        return new NFSInstanceInitializer();
    }

    public NFSInstanceInitializer uri(String uri) {
        this.uri = uri;
        return this;
    }

    public NFSInstanceInitializer cachePolicy(CachePolicy cachePolicy) {
        this.cachePolicy = cachePolicy;
        return this;
    }

    public FileSystem initialize() throws IOException {
        CacheManager cacheManager = null;
        FileSystem nfsFileSystem = null;
        try {
            cacheManager = FileMergeCacheManager.createMergeCacheManager(Engine.NFS);
            URI nfsFileUri = URI.create(this.uri);
            nfsFileSystem = createNFSFileSystem(
                nfsFileUri,
                cachePolicy == CachePolicy.META_CACHE || cachePolicy == CachePolicy.META_AND_DATA_CACHE
            );
            Configuration factoryConfig = new Configuration();
            final boolean validationEnabled = FileConfig.getInstance().getCacheConfig().isValidationEnabled();

            return new FileMergeCachingFileSystem(
                nfsFileSystem.getUri(),
                factoryConfig,
                cacheManager,
                nfsFileSystem,
                validationEnabled,
                true
            );
        } catch (Throwable t) {
            if (cacheManager != null) {
                try {
                    cacheManager.close();
                } catch (Throwable t1) {
                    // ignore
                }
            }
            if (nfsFileSystem != null) {
                try {
                    nfsFileSystem.close();
                } catch (Throwable t1) {
                    // ignore
                }
            }
            throw GeneralUtil.nestedException("Fail to create NFS file system!", t);
        }
    }

    private synchronized NFSFileSystem createNFSFileSystem(URI nfsUri, boolean enableCache) throws IOException {
        Map<String, Long> globalVariables = InstConfUtil.fetchLongConfigs(
            ConnectionParams.OSS_FS_MAX_READ_RATE,
            ConnectionParams.OSS_FS_MAX_WRITE_RATE
        );
        Long maxReadRate = Optional.ofNullable(globalVariables.get(ConnectionProperties.OSS_FS_MAX_READ_RATE))
            .orElse(StringNumericParser.simplyParseLong(ConnectionParams.OSS_FS_MAX_READ_RATE.getDefault()));
        Long maxWriteRate = Optional.ofNullable(globalVariables.get(ConnectionProperties.OSS_FS_MAX_WRITE_RATE))
            .orElse(StringNumericParser.simplyParseLong(ConnectionParams.OSS_FS_MAX_WRITE_RATE.getDefault()));

        FileSystemRateLimiter rateLimiter = new GuavaFileSystemRateLimiter(maxReadRate, maxWriteRate);
        NFSFileSystem nfsFileSystem = new NFSFileSystem(enableCache, rateLimiter);

        nfsFileSystem.initialize(nfsUri, new Configuration());
        return nfsFileSystem;
    }
}
