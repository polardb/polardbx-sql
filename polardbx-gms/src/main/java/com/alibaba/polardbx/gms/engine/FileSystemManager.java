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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.filesystem.FileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.RateLimitable;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class FileSystemManager {
    private ThreadPoolExecutor executor;

    private static volatile FileSystemManager instance;

    public static FileSystemManager getInstance() {
        if (instance == null) {
            synchronized (FileSystemManager.class) {
                if (instance == null) {
                    instance = new FileSystemManager();
                }
            }
        }
        return instance;
    }

    private LoadingCache<Engine, Optional<FileSystemGroup>> cache;

    private FileSystemManager() {
        cache = CacheBuilder.newBuilder()
            .removalListener(new EngineFileSystemRemovalListener())
            .build(new EngineFileSystemCacheLoader());
        this.executor = new ThreadPoolExecutor(16,
            16,
            1800,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(32),
            new NamedThreadFactory("FileSystemManager executor", true),
            new ThreadPoolExecutor.CallerRunsPolicy());

        MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getFileStorageInfoDataId(), null);
        MetaDbConfigManager.getInstance()
            .bindListener(MetaDbDataIdBuilder.getFileStorageInfoDataId(), new FileStorageInfoListener());
    }

    public ThreadPoolExecutor getExecutor() {
        return executor;
    }

    public LoadingCache<Engine, Optional<FileSystemGroup>> getCache() {
        return cache;
    }

    public static FileSystemGroup getFileSystemGroup(Engine engine) {
        return getFileSystemGroup(engine, true);
    }

    public static FileSystemGroup getFileSystemGroup(Engine engine, boolean throwException) {
        try {
            Optional<FileSystemGroup> optional = getInstance().getCache().get(engine);
            if (!optional.isPresent()) {
                if (!throwException) {
                    return null;
                } else {
                    // otherwise we get NPE when datasource not found.
                    throw GeneralUtil.nestedException("There is no datasource for engine: " + engine);
                }
            }
            return optional.get();
        } catch (ExecutionException e) {
            // catch execution exception when loading caches.
            throw GeneralUtil.nestedException(e);
        }
    }

    public static void invalidFileSystem() {
        for (Map.Entry<Engine, Optional<FileSystemGroup>> entry : getInstance().getCache().asMap().entrySet()) {
            Engine engine = entry.getKey();
            getInstance().getCache().invalidate(engine);
        }
    }

    public static void resetRate() {
        // fetch the rate params
        Map<String, Long> configs = InstConfUtil.fetchLongConfigs(
            ConnectionParams.OSS_FS_MAX_READ_RATE,
            ConnectionParams.OSS_FS_MAX_WRITE_RATE
        );
        final Long readRate = Optional.ofNullable(configs.get(ConnectionProperties.OSS_FS_MAX_READ_RATE))
            .orElse(StringNumericParser.simplyParseLong(ConnectionParams.OSS_FS_MAX_READ_RATE.getDefault()));
        final Long writeRate = Optional.ofNullable(configs.get(ConnectionProperties.OSS_FS_MAX_WRITE_RATE))
            .orElse(StringNumericParser.simplyParseLong(ConnectionParams.OSS_FS_MAX_WRITE_RATE.getDefault()));

        for (Engine engine : Engine.values()) {
            if (Engine.hasCache(engine)) {
                // If the engine does not exist, just skip
                FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(engine, false);
                // reset rate-limiter of master && slave file system.
                if (fileSystemGroup == null) {
                    continue;
                }
                FileMergeCachingFileSystem masterFs = (FileMergeCachingFileSystem) fileSystemGroup.getMaster();
                reloadRateLimiter(readRate, writeRate, masterFs);
                for (FileSystem fs : fileSystemGroup.getSlaves()) {
                    FileMergeCachingFileSystem slaveFs = (FileMergeCachingFileSystem) fs;
                    reloadRateLimiter(readRate, writeRate, slaveFs);
                }
            }
        }
    }

    private static void reloadRateLimiter(Long readRate, Long writeRate, FileMergeCachingFileSystem cachingFileSystem) {
        if (cachingFileSystem.getDataTier() instanceof RateLimitable) {
            FileSystemRateLimiter rateLimiter = ((RateLimitable) cachingFileSystem.getDataTier()).getRateLimiter();
            rateLimiter.setReadRate(readRate);
            rateLimiter.setWriteRate(writeRate);
        }
    }

    private static FileStorageInfoRecord queryLatest(Engine engine) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            FileStorageInfoAccessor fileStorageInfoAccessor = new FileStorageInfoAccessor();
            fileStorageInfoAccessor.setConnection(connection);

            return fileStorageInfoAccessor.queryLatest(engine);

        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private static List<FileStorageInfoRecord> query(Engine engine) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            FileStorageInfoAccessor fileStorageInfoAccessor = new FileStorageInfoAccessor();
            fileStorageInfoAccessor.setConnection(connection);

            return fileStorageInfoAccessor.query(engine);

        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private static class EngineFileSystemRemovalListener implements RemovalListener<Engine, Optional<FileSystemGroup>> {
        @Override
        public void onRemoval(RemovalNotification<Engine, Optional<FileSystemGroup>> notification) {
            Optional<FileSystemGroup> fsg = notification.getValue();
            if (fsg.isPresent()) {
                try {
                    fsg.get().close();
                } catch (IOException e) {
                    throw GeneralUtil.nestedException(e);
                }
            }
        }
    }

    private static class EngineFileSystemCacheLoader extends CacheLoader<Engine, Optional<FileSystemGroup>> {
        @Override
        public Optional<FileSystemGroup> load(Engine engine) throws Exception {
            List<FileStorageInfoRecord> records = query(engine);
            if (records == null || records.isEmpty()) {
                // No file storage info for this engine type.
                return Optional.empty();
            }
            FileSystem master = buildFileSystem(records.get(0));
            if (master == null) {
                // No file system implementation.
                return Optional.empty();
            }
            List<FileSystem> slaves = new ArrayList<>();
            if (!PasswdUtil.existsAkSkInEnv() && records.size() > 1) {
                for (int i = 1; i < records.size(); i++) {
                    FileSystem slave = buildFileSystem(records.get(i));
                    slaves.add(slave);
                }
            }
            return Optional.of(new FileSystemGroup(master, slaves, getInstance().executor,
                DeletePolicy.MAP.get(records.get(0).deletePolicy), records.get(0).status == 2));
        }
    }

    public static FileSystem buildFileSystem(FileStorageInfoRecord record) throws IOException {
        Engine engine = Engine.of(record.getEngine());
        switch (engine) {
        case OSS: {
            List<String> endpoints =
                ImmutableList.of(record.externalEndpoint, record.internalClassicEndpoint, record.internalVpcEndpoint);
            int endpointOrdinal = (int) Math.min(record.endpointOrdinal, endpoints.size() - 1);
            String accessKeyId =
                PasswdUtil.existsAkSkInEnv() ? PasswdUtil.decryptByKey(System.getenv(PasswdUtil.OSS_ACCESS_KEY_ID),
                    System.getenv(PasswdUtil.CIPHER_KEY)) : record.accessKeyId;
            String accessKeySecret =
                PasswdUtil.existsAkSkInEnv() ? PasswdUtil.decryptByKey(System.getenv(PasswdUtil.OSS_ACCESS_KEY_SECRET),
                    System.getenv(PasswdUtil.CIPHER_KEY)) :
                    PasswdUtil.decrypt(record.accessKeySecret);
            FileSystem ossFileSystem =
                OSSInstanceInitializer.newBuilder()
                    .accessKeyIdValue(accessKeyId)
                    .accessKeySecretValue(accessKeySecret)
                    .bucketName(record.fileUri)
                    .cachePolicy(CachePolicy.MAP.get(record.cachePolicy))
                    .endpointValue(endpoints.get(endpointOrdinal))
                    .initialize();
            return fileSystemWithDefaultDirectory(record.fileUri, ossFileSystem);
        }
        case LOCAL_DISK: {
            Configuration configuration = new Configuration();
            configuration.setBoolean("fs.file.impl.disable.cache", true);
            FileSystem localFileSystem = FileSystem.get(
                URI.create(record.fileUri), configuration
            );
            return fileSystemWithDefaultDirectory(record.fileUri, localFileSystem);
        }
        case EXTERNAL_DISK: {
            FileSystem externalDiskFileSystem =
                ExternalDiskInstanceInitializer.newBuilder()
                    .uri(record.fileUri)
                    .cachePolicy(CachePolicy.MAP.get(record.cachePolicy))
                    .initialize();
            return fileSystemWithDefaultDirectory(record.fileUri, externalDiskFileSystem);
        }
        case NFS: {
            FileSystem nfsFileSystem =
                NFSInstanceInitializer.newBuilder()
                    .uri(record.fileUri)
                    .cachePolicy(CachePolicy.MAP.get(record.cachePolicy))
                    .initialize();
            return fileSystemWithDefaultDirectory(record.fileUri, nfsFileSystem);
        }
        case S3: {
            FileSystem s3FileSystem =
                S3InstanceInitializer.newBuilder()
                    .accessKeyIdValue(record.accessKeyId)
                    .accessKeySecretValue(PasswdUtil.decrypt(record.accessKeySecret))
                    .bucketName(record.fileUri)
                    .cachePolicy(CachePolicy.MAP.get(record.cachePolicy))
                    .initialize();
            return fileSystemWithDefaultDirectory(record.fileUri, s3FileSystem);
        }
        case ABS: {
            FileSystem absFileSystem =
                ABSInstanceInitializer.newBuilder()
                    .endpointValue(record.externalEndpoint)
                    .accessKeyIdValue(record.accessKeyId)
                    .accessKeySecretValue(PasswdUtil.decrypt(record.accessKeySecret))
                    .bucketName(record.fileUri)
                    .cachePolicy(CachePolicy.MAP.get(record.cachePolicy))
                    .initialize();
            return fileSystemWithDefaultDirectory(record.fileUri, absFileSystem);
        }
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, "bad engine = " + engine);
        }
    }

    @NotNull
    private static FileSystem fileSystemWithDefaultDirectory(String uri, FileSystem fileSystem) {
        Path workingDirectory =
            new Path(URI.create(uri + ServerInstIdManager.getInstance().getMasterInstId() + "/"));
        try {
            fileSystem.setWorkingDirectory(workingDirectory);
        } catch (Throwable t) {
            try {
                fileSystem.close();
            } catch (Throwable t1) {
                // ignore
            }
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, t, "bad fileUri = " + uri);
        }
        return fileSystem;
    }

    static public Engine getDefaultColumnarEngine() {
        if (getFileSystemGroup(Engine.OSS, false) != null) {
            return Engine.OSS;
        } else if (getFileSystemGroup(Engine.EXTERNAL_DISK, false) != null) {
            return Engine.EXTERNAL_DISK;
        } else if (getFileSystemGroup(Engine.NFS, false) != null) {
            return Engine.NFS;
        } else if (getFileSystemGroup(Engine.S3, false) != null) {
            return Engine.S3;
        } else if (getFileSystemGroup(Engine.ABS, false) != null) {
            return Engine.LOCAL_DISK;
        } else {
            return Engine.OSS;
        }
    }
}
