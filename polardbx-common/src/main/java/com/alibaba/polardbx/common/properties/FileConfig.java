package com.alibaba.polardbx.common.properties;

import com.alibaba.polardbx.common.oss.filesystem.cache.CacheConfig;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import io.airlift.slice.DataSize;
import io.airlift.slice.Duration;

import java.nio.file.Path;
import java.nio.file.Paths;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.MPP_SPILL_PATHS;
import static com.alibaba.polardbx.common.properties.DynamicConfig.parseValue;
import static io.airlift.slice.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileConfig {

    private static final Logger log = LoggerFactory.getLogger(FileConfig.class);

    private static final String TEMP_DIR_NAME = "temp";

    private static final String CACHE_DIR_NAME = "cache";

    private Path rootPath;

    // root path of spillers
    private Path spillerTempPath;
    // root path of spillers
    private Path spillerCachePath;

    private final SpillConfig spillConfig = new SpillConfig();
    private final CacheConfig cacheConfig = new CacheConfig();
    private final FileMergeCacheConfig mergeCacheConfig = new FileMergeCacheConfig();

    private FileConfig() {
        initDirs(Paths.get("../spill"));
    }

    private void initDirs(Path spillerRootPath) {
        this.rootPath = spillerRootPath;
        // init temp paths
        this.spillerTempPath = Paths.get(spillerRootPath.toFile().getAbsolutePath(), TEMP_DIR_NAME);
        log.info("init SpillerManager with temp path:" + spillerTempPath.toFile().getAbsolutePath());
        this.spillerCachePath = Paths.get(spillerRootPath.toFile().getAbsolutePath(), CACHE_DIR_NAME);
        log.info("init SpillerManager with cache path:" + spillerCachePath.toFile().getAbsolutePath());
        spillerRootPath.toFile().mkdirs();
        spillerTempPath.toFile().mkdirs();
        spillerCachePath.toFile().mkdirs();
        this.cacheConfig.setBaseDirectory(spillerCachePath.toUri());
    }

    public Path getRootPath() {
        return rootPath;
    }

    public Path getSpillerTempPath() {
        return spillerTempPath;
    }

    public CacheConfig getCacheConfig() {
        return cacheConfig;
    }

    public FileMergeCacheConfig getMergeCacheConfig() {
        return mergeCacheConfig;
    }

    public SpillConfig getSpillConfig() {
        return spillConfig;
    }

    public void loadValue(org.slf4j.Logger logger, String key, String value) {
        if (key != null && value != null) {
            switch (key.toUpperCase()) {
            case ConnectionProperties.OSS_FS_CACHE_TTL:
                mergeCacheConfig.setCacheTtl(new Duration(parseValue(value, Long.class, 2L), DAYS));
                break;
            case ConnectionProperties.OSS_FS_MAX_CACHED_ENTRIES:
                mergeCacheConfig.setMaxCachedEntries(parseValue(value, Integer.class, 2048));
                break;
            case ConnectionProperties.OSS_FS_ENABLE_CACHED:
                mergeCacheConfig.setEnableCache(parseValue(value, Boolean.class, true));
                break;
            case ConnectionProperties.OSS_FS_MAX_CACHED_GB:
                mergeCacheConfig.setMaxInDiskCacheSize(new DataSize(parseValue(value, Integer.class, 100), GIGABYTE));
                break;
            case ConnectionProperties.OSS_FS_USE_BYTES_CACHE:
                mergeCacheConfig.setUseByteCache(parseValue(value, Boolean.class, false));
                break;
            case ConnectionProperties.OSS_FS_MEMORY_RATIO_OF_BYTES_CACHE:
                mergeCacheConfig.setMemoryRatioOfBytesCache(parseValue(value, Double.class, 0.3d));
                break;
            case ConnectionProperties.OSS_FS_VALIDATION_ENABLE:
                cacheConfig.setValidationEnabled(parseValue(value, Boolean.class, false));
                break;
            case ConnectionProperties.OSS_FS_CACHED_FLUSH_THREAD_NUM:
                cacheConfig.setFlushCacheThreadNum(parseValue(value, Integer.class, ThreadCpuStatUtil.NUM_CORES));
                break;
            case MPP_SPILL_PATHS:
                initDirs(Paths.get(value));
                break;
            default:
                spillConfig.loadValue(logger, key, value);
                break;
            }
        }
    }

    private static final FileConfig instance = new FileConfig();

    public static FileConfig getInstance() {
        return instance;
    }

}


