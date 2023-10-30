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
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.filesystem.FileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.NFSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheType;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
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

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

import static com.google.common.base.Preconditions.checkState;

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

    private Map<Engine, StampedLock> lockMap;

    private FileSystemManager() {
        lockMap = new ConcurrentHashMap<>();
        for (Engine engine : Engine.values()) {
            lockMap.put(engine, new StampedLock());
        }
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

    private StampedLock getLockImpl(Engine engine) {
        return lockMap.get(engine);
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

    private static StampedLock getLock(Engine engine) {
        return getInstance().getLockImpl(engine);
    }

    public static int getReadLockCount(Engine engine) {
        return getLock(engine).getReadLockCount();
    }

    public static boolean isWriteLocked(Engine engine) {
        return getLock(engine).isWriteLocked();
    }

    public static long readLockWithTimeOut(Engine engine) {
        try {
            long stamp = getInstance().getLockImpl(engine).tryReadLock(3, TimeUnit.SECONDS);
            if (stamp == 0) {
                throw new RuntimeException("get read lock timeout");
            }
            return stamp;
        } catch (InterruptedException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public static void unlockRead(Engine engine, long stamp) {
        getInstance().getLockImpl(engine).unlockRead(stamp);
    }

    public static void invalidFileSystem() {
        for (Map.Entry<Engine, Optional<FileSystemGroup>> entry : getInstance().getCache().asMap().entrySet()) {
            Engine engine = entry.getKey();
            long stamp = getLock(engine).writeLock();
            try {
                getInstance().getCache().invalidate(engine);
            } finally {
                getLock(engine).unlockWrite(stamp);
            }
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

        FileSystemGroup fileSystemGroup = getFileSystemGroup(Engine.OSS);

        // reset rate-limiter of master && slave file system.
        FileMergeCachingFileSystem masterFs = (FileMergeCachingFileSystem) fileSystemGroup.getMaster();
        if (masterFs.getDataTier() instanceof OSSFileSystem) {
            FileSystemRateLimiter rateLimiter = ((OSSFileSystem) masterFs.getDataTier()).getRateLimiter();
            rateLimiter.setReadRate(readRate);
            rateLimiter.setWriteRate(writeRate);
        } else if (masterFs.getDataTier() instanceof NFSFileSystem) {
            FileSystemRateLimiter rateLimiter = ((NFSFileSystem) masterFs.getDataTier()).getRateLimiter();
            rateLimiter.setReadRate(readRate);
            rateLimiter.setWriteRate(writeRate);
        }
        for (FileSystem fs : fileSystemGroup.getSlaves()) {
            FileMergeCachingFileSystem slaveFs = (FileMergeCachingFileSystem) fs;
            if (slaveFs.getDataTier() instanceof OSSFileSystem) {
                FileSystemRateLimiter rateLimiter = ((OSSFileSystem) slaveFs.getDataTier()).getRateLimiter();
                rateLimiter.setReadRate(readRate);
                rateLimiter.setWriteRate(writeRate);
            }
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
            if (records.size() > 1) {
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
            FileSystem ossFileSystem =
                OSSInstanceInitializer.newBuilder()
                    .accessKeyIdValue(record.accessKeyId)
                    .accessKeySecretValue(PasswdUtil.decrypt(record.accessKeySecret))
                    .bucketName(record.fileUri)
                    .cachePolicy(CachePolicy.MAP.get(record.cachePolicy))
                    .endpointValue(endpoints.get(endpointOrdinal))
                    .initialize();
            Path workingDirectory =
                new Path(URI.create(record.fileUri + ServerInstIdManager.getInstance().getMasterInstId() + "/"));
            try {
                ossFileSystem.setWorkingDirectory(workingDirectory);
            } catch (Throwable t) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "bad fileUri = " + record.fileUri, t);
            }
            return ossFileSystem;
        }
        case LOCAL_DISK: {
            Configuration configuration = new Configuration();
            configuration.setBoolean("fs.file.impl.disable.cache", true);
            FileSystem localFileSystem = FileSystem.get(
                URI.create(record.fileUri), configuration
            );
            Path workingDirectory =
                new Path(URI.create(record.fileUri + ServerInstIdManager.getInstance().getMasterInstId() + "/"));
            try {
                localFileSystem.setWorkingDirectory(workingDirectory);
            } catch (Throwable t) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "bad fileUri = " + record.fileUri, t);
            }
            return localFileSystem;
        }
        case EXTERNAL_DISK: {
            Configuration configuration = new Configuration();
            configuration.setBoolean("fs.file.impl.disable.cache", true);
            FileSystem externalFileSystem = FileSystem.get(
                URI.create(record.fileUri), configuration
            );
            FileSystem fileSystem;
            // TODO: catch exception
            CacheManager cacheManager = FileMergeCacheManager.createMergeCacheManager(InstConfUtil.fetchLongConfigs(
                ConnectionParams.OSS_FS_CACHE_TTL,
                ConnectionParams.OSS_FS_MAX_CACHED_ENTRIES
            ), engine);

            Configuration factoryConfig = new Configuration();
            final CacheType cacheType = CacheType.FILE_MERGE;
            final boolean validationEnabled = false;

            checkState(cacheType != null);

            switch (cacheType) {
            case FILE_MERGE:
                fileSystem = new FileMergeCachingFileSystem(
                    URI.create(record.fileUri),
                    factoryConfig,
                    cacheManager,
                    externalFileSystem,
                    validationEnabled,
                    true);
                break;
            default:
                throw new IllegalArgumentException("Invalid CacheType: " + cacheType.name());
            }
            Path workingDirectory =
                new Path(URI.create(record.fileUri + ServerInstIdManager.getInstance().getMasterInstId() + "/"));
            try {
                fileSystem.setWorkingDirectory(workingDirectory);
            } catch (Throwable t) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "bad fileUri = " + record.fileUri, t);
            }
            return fileSystem;
        }
        case NFS: {
            FileSystem nfsFileSystem = NFSInstanceInitializer.newBuilder().uri(record.fileUri)
                .cachePolicy(CachePolicy.MAP.get(record.cachePolicy)).initialize();
            Path workingDirectory =
                new Path(URI.create(record.fileUri + ServerInstIdManager.getInstance().getMasterInstId() + "/"));
            try {
                nfsFileSystem.setWorkingDirectory(workingDirectory);
            } catch (Throwable t) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "bad fileUri = " + record.fileUri, t);
            }
            return nfsFileSystem;
        }
        default:
            return null;
        }
    }

}
