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

import com.alibaba.polardbx.common.oss.filesystem.FetchPolicy;
import com.alibaba.polardbx.common.oss.filesystem.FileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.GuavaFileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheConfig;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheStats;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheType;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheConfig;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import io.airlift.slice.DataSize;
import io.airlift.slice.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.alibaba.polardbx.common.oss.filesystem.Constants.ACCESS_KEY_ID;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.ACCESS_KEY_SECRET;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.ENDPOINT_KEY;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.OSS_FETCH_POLICY;
import static com.alibaba.polardbx.common.oss.filesystem.cache.CacheQuotaScope.GLOBAL;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OSSInstanceInitializer {
    /**
     * oss://[accessKeyId:accessKeySecret@]bucket[.endpoint]/object/path
     */
    public static final String URI_FORMAT = "oss://%s/%s";
    public static final String CACHE_FILE_PREFIX = "cache";
    public static final String OSS_CACHE_FLUSHER_THREAD_NAME = "oss-cache-flusher";
    public static final String OSS_CACHE_REMOVER_THREAD_NAME = "oss-cache-remover";
    public static final String OSS_CACHE_SIZE_CALCULATOR_THREAD_NAME = "oss-cache-size-calculator";

    public String accessKeyIdValue;
    public String accessKeySecretValue;
    public String bucketUri;
    public String endpointValue;
    public CachePolicy cachePolicy;

    public OSSInstanceInitializer() {
    }

    public static OSSInstanceInitializer newBuilder() {
        return new OSSInstanceInitializer();
    }

    public OSSInstanceInitializer accessKeyIdValue(String accessKeyIdValue) {
        this.accessKeyIdValue = accessKeyIdValue;
        return this;
    }

    public OSSInstanceInitializer accessKeySecretValue(String accessKeySecretValue) {
        this.accessKeySecretValue = accessKeySecretValue;
        return this;
    }

    public OSSInstanceInitializer bucketName(String bucketUri) {
        this.bucketUri = bucketUri;
        return this;
    }

    public OSSInstanceInitializer endpointValue(String endpointValue) {
        this.endpointValue = endpointValue;
        return this;
    }

    public OSSInstanceInitializer cachePolicy(CachePolicy cachePolicy) {
        this.cachePolicy = cachePolicy;
        return this;
    }

    public FileSystem initialize() {
        FileSystem fileSystem;
        CacheManager cacheManager;
        try {
            Map<String, Long> globalVariables = InstConfUtil.fetchLongConfigs(
                ConnectionParams.OSS_FS_CACHE_TTL,
                ConnectionParams.OSS_FS_MAX_CACHED_ENTRIES
            );
            cacheManager = createCacheManger(globalVariables);
        } catch (IOException e) {
            throw GeneralUtil.nestedException("Fail to create cache manager!");
        }

        URI ossFileUri = URI.create(this.bucketUri);
        FileSystem ossFileSystem;
        try {
            ossFileSystem = createOSSFileSystem(ossFileUri,
                cachePolicy == CachePolicy.META_CACHE || cachePolicy == CachePolicy.META_AND_DATA_CACHE);
        } catch (IOException e) {
            throw GeneralUtil.nestedException("Fail to create file system!");
        }

        URI fsUri = ossFileSystem.getUri();
        Configuration factoryConfig = new Configuration();
        final CacheType cacheType = CacheType.FILE_MERGE;
        final boolean validationEnabled = false;

        checkState(cacheType != null);

        switch (cacheType) {
        case FILE_MERGE:
            fileSystem = new FileMergeCachingFileSystem(
                fsUri,
                factoryConfig,
                cacheManager,
                ossFileSystem,
                validationEnabled,
                cachePolicy == CachePolicy.META_AND_DATA_CACHE || cachePolicy == CachePolicy.DATA_CACHE);
            break;
        default:
            throw new IllegalArgumentException("Invalid CacheType: " + cacheType.name());
        }

        return fileSystem;
    }

    private synchronized OSSFileSystem createOSSFileSystem(URI ossFileUri, boolean enableCache) throws
        IOException {
        // fetch rate params
        Map<String, Long> globalVariables = InstConfUtil.fetchLongConfigs(
            ConnectionParams.OSS_FS_MAX_READ_RATE,
            ConnectionParams.OSS_FS_MAX_WRITE_RATE
        );
        Long maxReadRate = Optional.ofNullable(globalVariables.get(ConnectionProperties.OSS_FS_MAX_READ_RATE))
            .orElse(StringNumericParser.simplyParseLong(ConnectionParams.OSS_FS_MAX_READ_RATE.getDefault()));
        Long maxWriteRate = Optional.ofNullable(globalVariables.get(ConnectionProperties.OSS_FS_MAX_WRITE_RATE))
            .orElse(StringNumericParser.simplyParseLong(ConnectionParams.OSS_FS_MAX_WRITE_RATE.getDefault()));

        // oss file system
        // oss://[accessKeyId:accessKeySecret@]bucket[.endpoint]/object/path
        FileSystemRateLimiter rateLimiter = new GuavaFileSystemRateLimiter(maxReadRate, maxWriteRate);
        OSSFileSystem OSS_FILE_SYSTEM = new OSSFileSystem(enableCache, rateLimiter);
        Configuration fsConf = new Configuration();
        fsConf.set(ACCESS_KEY_ID, this.accessKeyIdValue);
        fsConf.set(ACCESS_KEY_SECRET, this.accessKeySecretValue);
        fsConf.set(ENDPOINT_KEY, this.endpointValue);
        fsConf.set(OSS_FETCH_POLICY, FetchPolicy.REQUESTED.name());
        OSS_FILE_SYSTEM.initialize(ossFileUri, fsConf);
        return OSS_FILE_SYSTEM;

    }

    private synchronized CacheManager createCacheManger(Map<String, Long> globalVariables) throws IOException {

        CacheConfig cacheConfig = new CacheConfig();
        FileMergeCacheConfig fileMergeCacheConfig = new FileMergeCacheConfig();
        CacheStats cacheStats = new CacheStats();

        Long cacheTTL = Optional.ofNullable(globalVariables.get(ConnectionProperties.OSS_FS_CACHE_TTL))
            .orElse(StringNumericParser.simplyParseLong(ConnectionParams.OSS_FS_CACHE_TTL.getDefault()));
        Long maxCacheEntries = Optional.ofNullable(globalVariables.get(ConnectionProperties.OSS_FS_MAX_CACHED_ENTRIES))
            .orElse(StringNumericParser.simplyParseLong(ConnectionParams.OSS_FS_MAX_CACHED_ENTRIES.getDefault()));

        cacheConfig.setBaseDirectory(Files.createTempDirectory(Paths.get("../spill/temp"), CACHE_FILE_PREFIX).toUri());
        cacheConfig.setCacheQuotaScope(GLOBAL);
        cacheConfig.setCacheType(CacheType.FILE_MERGE);

        cacheConfig.setCachingEnabled(true);
        cacheConfig.setValidationEnabled(false);

        fileMergeCacheConfig.setCacheTtl(new Duration(cacheTTL, DAYS));
        fileMergeCacheConfig.setMaxCachedEntries(maxCacheEntries.intValue());
        fileMergeCacheConfig.setMaxInMemoryCacheSize(new DataSize(2, GIGABYTE));

        fileMergeCacheConfig.setHotCacheTtl(new Duration(3, SECONDS));
        fileMergeCacheConfig.setMaxHotCachedEntries(1000);

        final int cores = ThreadCpuStatUtil.NUM_CORES;
        ScheduledExecutorService cacheFlushExecutor =
            newScheduledThreadPool(cores, new NamedThreadFactory(OSS_CACHE_FLUSHER_THREAD_NAME));
        ScheduledExecutorService cacheRemovalExecutor =
            newScheduledThreadPool(cores, new NamedThreadFactory(OSS_CACHE_REMOVER_THREAD_NAME));
        ScheduledExecutorService cacheSizeCalculateExecutor =
            newScheduledThreadPool(cores, new NamedThreadFactory(OSS_CACHE_SIZE_CALCULATOR_THREAD_NAME));

        CacheManager cacheManager = new FileMergeCacheManager(
            cacheConfig,
            fileMergeCacheConfig,
            cacheStats,
            cacheFlushExecutor,
            cacheRemovalExecutor,
            cacheSizeCalculateExecutor
        );
        return cacheManager;
    }

}
