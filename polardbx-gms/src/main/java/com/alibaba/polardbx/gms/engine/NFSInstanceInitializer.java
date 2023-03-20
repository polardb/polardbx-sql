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

    public FileSystem initialize() {
        FileSystem fileSystem;
        CacheManager cacheManager;
        try {
            Map<String, Long> globalVariables = InstConfUtil.fetchLongConfigs(
                ConnectionParams.OSS_FS_CACHE_TTL,
                ConnectionParams.OSS_FS_MAX_CACHED_ENTRIES
            );
            cacheManager = FileMergeCacheManager.createMergeCacheManager(globalVariables, Engine.NFS);
        } catch (IOException e) {
            throw GeneralUtil.nestedException("Failed to create cache manager!");
        }

        URI nfsFileUri = URI.create(this.uri);
        FileSystem nfsFileSystem;
        try {
            nfsFileSystem = createNFSFileSystem(nfsFileUri,
                cachePolicy == CachePolicy.META_CACHE || cachePolicy == CachePolicy.META_AND_DATA_CACHE);
        } catch (IOException e) {
            throw GeneralUtil.nestedException("Fail to create file system!");
        }

        Configuration factoryConfig = new Configuration();
        final CacheType cacheType = CacheType.FILE_MERGE;
        final boolean validationEnabled = false;

        switch (cacheType) {
        case FILE_MERGE:
            fileSystem = new FileMergeCachingFileSystem(
                nfsFileSystem.getUri(),
                factoryConfig,
                cacheManager,
                nfsFileSystem,
                validationEnabled,
                cachePolicy == CachePolicy.META_AND_DATA_CACHE || cachePolicy == CachePolicy.DATA_CACHE);
            break;
        default:
            throw new IllegalArgumentException("Invalid CacheType: " + cacheType.name());
        }

        return fileSystem;
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
