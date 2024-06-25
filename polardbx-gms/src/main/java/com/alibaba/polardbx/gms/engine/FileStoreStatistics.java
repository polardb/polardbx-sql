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
import com.alibaba.polardbx.common.oss.filesystem.Constants;
import com.alibaba.polardbx.common.oss.filesystem.FileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.NFSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.RateLimitable;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheConfig;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheStats;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheConfig;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobalStorageStatistics;
import org.apache.hadoop.fs.StorageStatistics;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class FileStoreStatistics {

    private static long lastRecordTime = 0L;
    private static List<Map<StatisticItem, String>> PREVIOUS_STATS = null;

    private static final int FILE_STORAGE_FIELD_COUNT = 10;

    public static final int CACHE_STATS_FIELD_COUNT = 13;

    private static List<Map<StatisticItem, String>> collectStatistics() {
        List<Map<StatisticItem, String>> localStats = new ArrayList<>();
        for (Engine engine : Engine.values()) {
            // for each file store engine
            if (!Engine.isFileStore(engine)) {
                continue;
            }

            FileSystemGroup group = FileSystemManager.getFileSystemGroup(engine, false);
            if (group != null) {
                FileSystem master = group.getMaster();
                // do collect
                foreachFileSystem(master, engine, true, localStats);

                for (FileSystem slave : group.getSlaves()) {
                    // do collect
                    foreachFileSystem(slave, engine, false, localStats);
                }
            }
        }
        return localStats;
    }

    private static void foreachFileSystem(FileSystem fileSystem, Engine engine, boolean isMaster,
                                          List<Map<StatisticItem, String>> localStats) {
        ImmutableMap.Builder<StatisticItem, String> statsBuilder = ImmutableMap.builder();
        // basic info.
        String endpoint = fileSystem.getConf().get(Constants.ENDPOINT_KEY);

        statsBuilder.put(StatisticItem.ENGINE, String.valueOf(engine));
        statsBuilder.put(StatisticItem.ENDPOINT, endpoint == null ? "" : endpoint);
        statsBuilder.put(StatisticItem.FILE_URI, fileSystem.getWorkingDirectory().toString());
        statsBuilder.put(StatisticItem.IS_MASTER, String.valueOf(isMaster ? 1 : 0));

        // statistics info.
        String scheme = fileSystem.getScheme();
        if (scheme == null) {
            return;
        }
        GlobalStorageStatistics globalStorageStatistics = FileSystem.getGlobalStorageStatistics();
        StorageStatistics storageStatistics = globalStorageStatistics.get(scheme);
        if (storageStatistics == null) {
            return;
        }

        // record statistics info in the last period
        Iterator<StorageStatistics.LongStatistic> iterator = storageStatistics.getLongStatistics();
        while (iterator.hasNext()) {
            StorageStatistics.LongStatistic statistics = iterator.next();
            StatisticItem item = StatisticItem.of(statistics.getName());
            if (item == null) {
                continue;
            }
            statsBuilder.put(item, String.valueOf(statistics.getValue()));
        }

        if (fileSystem instanceof FileMergeCachingFileSystem) {
            FileSystem dataTierFileSystem = ((FileMergeCachingFileSystem) fileSystem).getDataTier();
            if (dataTierFileSystem instanceof RateLimitable) {
                FileSystemRateLimiter rateLimiter = ((RateLimitable) dataTierFileSystem).getRateLimiter();
                statsBuilder.put(StatisticItem.MAX_READ_RATE, String.valueOf(rateLimiter.getReadRate()));
                statsBuilder.put(StatisticItem.MAX_WRITE_RATE, String.valueOf(rateLimiter.getWriteRate()));
            } else {
                statsBuilder.put(StatisticItem.MAX_READ_RATE, String.valueOf(-1L));
                statsBuilder.put(StatisticItem.MAX_WRITE_RATE, String.valueOf(-1L));
            }
        } else {
            statsBuilder.put(StatisticItem.MAX_READ_RATE, String.valueOf(-1L));
            statsBuilder.put(StatisticItem.MAX_WRITE_RATE, String.valueOf(-1L));
        }
        localStats.add(statsBuilder.build());
    }

    public enum StatisticItem {
        ENGINE("engine"),
        ENDPOINT("endpoint"),
        FILE_URI("file_uri"),
        IS_MASTER("is_master"),
        BYTES_READ("bytesRead"),
        BYTES_WRITTEN("bytesWritten"),
        READ_OPS("readOps"),
        WRITE_OPS("writeOps"),
        MAX_READ_RATE("max_read_rate"),
        MAX_WRITE_RATE("max_write_rate");

        private String itemName;

        StatisticItem(String itemName) {
            this.itemName = itemName;
        }

        static StatisticItem of(String itemName) {
            for (StatisticItem item : values()) {
                if (item.itemName.equalsIgnoreCase(itemName)) {
                    return item;
                }
            }
            return null;
        }
    }

    public synchronized static List<byte[][]> generateFileStoragePacket(Map<String, Object> connectionVariables) {
        List<byte[][]> fileStorageRows = new ArrayList<>();
        List<Map<StatisticItem, String>> mapList = collectStatistics();

        boolean value = false;
        if (connectionVariables.containsKey(ConnectionProperties.ENABLE_FILE_STORAGE_DELTA_STATISTIC)) {
            value = Boolean.parseBoolean(connectionVariables.get(
                ConnectionProperties.ENABLE_FILE_STORAGE_DELTA_STATISTIC).toString());
        }

        for (int index = 0; index < mapList.size(); index++) {
            byte[][] result = new byte[FILE_STORAGE_FIELD_COUNT][];
            int pos = 0;

            Map<StatisticItem, String> current = mapList.get(index);
            // basic info.
            result[pos++] = bytesOfString(current.get(FileStoreStatistics.StatisticItem.ENGINE));
            result[pos++] = bytesOfString(current.get(FileStoreStatistics.StatisticItem.ENDPOINT));
            result[pos++] = bytesOfString(current.get(FileStoreStatistics.StatisticItem.FILE_URI));
            result[pos++] = bytesOfString(current.get(FileStoreStatistics.StatisticItem.IS_MASTER));
            // statistics info.

            /**
             * By default, the result values mean the total data volume,
             * but it will switch to outputting incremental values after executing the following command.
             *
             * set ENABLE_FILE_STORAGE_DELTA_STATISTIC=true;
             */
            if (value && PREVIOUS_STATS != null && PREVIOUS_STATS.get(index) != null) {
                long deltaTs = System.currentTimeMillis() - lastRecordTime;
                Map<StatisticItem, String> last = PREVIOUS_STATS.get(index);
                result[pos++] = deltaBytesOfStats(current.get(FileStoreStatistics.StatisticItem.BYTES_READ),
                    last.get(FileStoreStatistics.StatisticItem.BYTES_READ), deltaTs);
                result[pos++] = deltaBytesOfStats(current.get(FileStoreStatistics.StatisticItem.BYTES_WRITTEN),
                    last.get(FileStoreStatistics.StatisticItem.BYTES_WRITTEN), deltaTs);
                result[pos++] = deltaBytesOfStats(current.get(FileStoreStatistics.StatisticItem.READ_OPS),
                    last.get(FileStoreStatistics.StatisticItem.READ_OPS), deltaTs);
                result[pos++] = deltaBytesOfStats(current.get(FileStoreStatistics.StatisticItem.WRITE_OPS),
                    last.get(FileStoreStatistics.StatisticItem.WRITE_OPS), deltaTs);
            } else {
                result[pos++] = bytesOfString(current.get(FileStoreStatistics.StatisticItem.BYTES_READ));
                result[pos++] = bytesOfString(current.get(FileStoreStatistics.StatisticItem.BYTES_WRITTEN));
                result[pos++] = bytesOfString(current.get(FileStoreStatistics.StatisticItem.READ_OPS));
                result[pos++] = bytesOfString(current.get(FileStoreStatistics.StatisticItem.WRITE_OPS));
            }

            // rate limit info.
            result[pos++] = bytesOfString(current.get(StatisticItem.MAX_READ_RATE));
            result[pos++] = bytesOfString(current.get(StatisticItem.MAX_WRITE_RATE));

            fileStorageRows.add(result);
        }
        lastRecordTime = System.currentTimeMillis();
        PREVIOUS_STATS = mapList;
        return fileStorageRows;
    }

    private static HashMap<Engine, FileSystemGroup> getSystemEngine() {
        HashMap<Engine, FileSystemGroup> fsGroupMap = new HashMap<>();
        for (Engine engine : Engine.values()) {
            FileSystemGroup group = FileSystemManager.getFileSystemGroup(engine, false);
            if (group == null) {
                continue;
            }
            fsGroupMap.put(engine, group);
        }
        return fsGroupMap;
    }

    public synchronized static List<byte[][]> generateCacheStatsPacket() {

        List<byte[][]> cacheStatsResultsList = new ArrayList<>();

        Map<Engine, FileSystemGroup> rets = getSystemEngine();

        for (Map.Entry<Engine, FileSystemGroup> entry : rets.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }

            FileSystem fileSystem = entry.getValue().getMaster();
            if (fileSystem instanceof FileMergeCachingFileSystem) {
                CacheManager cacheManager = ((FileMergeCachingFileSystem) fileSystem).getCacheManager();

                if (cacheManager != null) {
                    CacheStats cacheStats = ((FileMergeCacheManager) cacheManager).getStats();
                    FileMergeCacheConfig fileMergeCacheConfig =
                        FileConfig.getInstance().getMergeCacheConfig();
                    CacheConfig cacheConfig = FileConfig.getInstance().getCacheConfig();
                    BigInteger cacheSize = ((FileMergeCacheManager) cacheManager).calcCacheSize();
                    long cacheEntries = ((FileMergeCacheManager) cacheManager).currentCacheEntries();

                    byte[][] results = new byte[CACHE_STATS_FIELD_COUNT][];
                    int pos = 0;
                    results[pos++] =
                        String.valueOf(entry.getKey()).getBytes();
                    results[pos++] = String.valueOf(cacheSize).getBytes();
                    results[pos++] = String.valueOf(cacheEntries).getBytes();
                    results[pos++] = String.valueOf(cacheStats.getInMemoryRetainedBytes()).getBytes();
                    results[pos++] = String.valueOf(cacheStats.getCacheHit()).getBytes();
                    results[pos++] = String.valueOf(0).getBytes();
                    results[pos++] = String.valueOf(cacheStats.getCacheMiss()).getBytes();
                    results[pos++] = String.valueOf(cacheStats.getQuotaExceed()).getBytes();
                    results[pos++] = String.valueOf(cacheStats.getCacheUnavailable()).getBytes();
                    results[pos++] = cacheConfig.getBaseDirectory().toString().getBytes();
                    results[pos++] = fileMergeCacheConfig.getCacheTtl().toString().getBytes();
                    results[pos++] = new StringBuilder().append(fileMergeCacheConfig.getMaxCachedEntries())
                        .append(" ENTRIES").toString().getBytes();
                    results[pos++] =
                        new StringBuilder().append(fileMergeCacheConfig.getMaxInDiskCacheSize().toBytes())
                            .append(" Bytes").toString().getBytes();

                    cacheStatsResultsList.add(results);

                    // Generate cache stats for compressed bytes cache.
                    if (fileMergeCacheConfig.isUseByteCache()) {
                        byte[][] bytesCachePacket = ((FileMergeCacheManager) cacheManager).generatePacketOfBytesCache();
                        cacheStatsResultsList.add(bytesCachePacket);
                    }
                }
            }
        }
        return cacheStatsResultsList;
    }

    public synchronized static List<byte[][]> generateCacheFileStatsPacket() {
        List<byte[][]> cacheStatsResultsList = new ArrayList<>();
        Map<Engine, FileSystemGroup> rets = getSystemEngine();
        for (Map.Entry<Engine, FileSystemGroup> entry : rets.entrySet()) {
            FileSystem fileSystem = entry.getValue().getMaster();
            if (fileSystem instanceof FileMergeCachingFileSystem) {
                CacheManager cacheManager = ((FileMergeCachingFileSystem) fileSystem).getCacheManager();

                if (cacheManager != null) {

                    cacheStatsResultsList.addAll(((FileMergeCacheManager) cacheManager).collectLocalFileStats());
                }
            }
        }
        return cacheStatsResultsList;
    }

    private static byte[] deltaBytesOfStats(String value, String lastValue, long delta) {
        if (TStringUtil.isEmpty(value) || TStringUtil.isEmpty(lastValue)) {
            return new byte[] {};
        }
        long stat = Long.valueOf(value);
        long lastStat = Long.valueOf(lastValue);
        double ret = (stat - lastStat) / 1000.0 / delta;
        return bytesOfString(String.valueOf(ret));
    }

    private static byte[] bytesOfString(String value) {
        return TStringUtil.isEmpty(value) ? new byte[] {} : value.getBytes();
    }
}
