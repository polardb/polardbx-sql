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

package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.Fields;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheStats;
import com.alibaba.polardbx.common.oss.filesystem.cache.CachingFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.gms.FileVersionStorage;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.PriorityExecutorInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.operator.ColumnarScanExec;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.EOFPacket;
import com.alibaba.polardbx.net.packet.FieldPacket;
import com.alibaba.polardbx.net.packet.ResultSetHeaderPacket;
import com.alibaba.polardbx.net.packet.RowDataPacket;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import com.alibaba.polardbx.server.util.LongUtil;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import org.apache.hadoop.fs.FileSystem;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * show @@columnar_r
 * 查看列存读取相关指标
 */
public final class ShowColumnarRead {

    private static final int FIELD_COUNT = 27;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("LATENCY_MS", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOGICAL_MEMORY_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOGICAL_MEMORY_MAX_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_CACHE_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_CACHE_MAX_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VERSION_CACHE_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VERSION_CACHE_MAX_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILESYSTEM_USED_SIZE_ON_DISK", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILESYSTEM_MAX_SIZE_ON_DISK", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILESYSTEM_CACHE_USED_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILESYSTEM_CACHE_MAX_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("BLOCK_CACHE_HIT_RATE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("VERSION_CACHE_HIT_RATE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FILESYSTEM_CACHE_HIT_RATE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("INCREMENT_OPEN_FD", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SNAPSHOT_OPEN_FD", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOADED_VERSION_FILES", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LOADED_VERSION_NUM", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("INCREMENT_FILE_REQUEST", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SNAPSHOT_FILE_REQUEST", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("OSS_READ_REQUEST", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("OSS_READ_SIZE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("IO_THREAD_QUEUE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SCAN_THREAD_QUEUE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TP_EXEC_QUEUE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("AP_EXEC_QUEUE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXEC_BLOCK_QUEUE", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c) {
        ByteBufferHolder buffer = c.allocate();
        IPacketOutputProxy proxy = PacketOutputProxyFactory.getInstance().createProxy(c, buffer);
        proxy.packetBegin();

        // write header
        proxy = header.write(proxy);

        // write fields
        for (FieldPacket field : fields) {
            proxy = field.write(proxy);
        }

        // write eof
        proxy = eof.write(proxy);

        // write rows
        byte packetId = eof.packetId;
        RowDataPacket row = getStats(c.getResultSetCharset());
        row.packetId = ++packetId;
        proxy = row.write(proxy);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        proxy = lastEof.write(proxy);

        // write buffer
        proxy.packetEnd();
    }

    private static RowDataPacket getStats(String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);

        FileVersionStorage fileVersionStorage = DynamicColumnarManager.getInstance().getVersionStorage();
        OSSFileSystem ossFs = getOssFileSystem();
        CacheManager ossCacheManager = getOssCacheManager();

        addLatency(row);
        addLogicalMemory(row);
        addBlockCache(row);
        addVersionCache(row, fileVersionStorage);
        addFileSystemOnDisk(row, ossCacheManager);
        addFileSystemInMemory(row, ossCacheManager);
        addBlockHitRate(row, charset);
        addVersionHitRate(row, charset, fileVersionStorage);
        addFsHitRate(row, charset, ossCacheManager);
        addFileDescriptor(row, fileVersionStorage);
        addLoadedVersion(row);
        addFileReq(row);
        addOssRead(row, ossFs);
        addColumnarScanPool(row);
        addExecPool(row);
        return row;
    }

    /**
     * LATENCY_MS
     */
    private static void addLatency(RowDataPacket row) {
        row.add(LongUtil.toBytesOrNull(getLatency()));
    }

    /**
     * latency of columnar version
     */
    private static Long getLatency() {
        // columnar node will write heartbeat tso
        Long tso = ColumnarTransactionUtils.getLatestTsoFromGms();

        if (tso == null) {
            return null;
        }

        long latestTimeMillis = ITimestampOracle.getTimeMillis(tso);
        long curTimeMillis = System.currentTimeMillis();
        return curTimeMillis - latestTimeMillis;
    }

    /**
     * stats from global memory pool:
     * LOGICAL_MEMORY_USED_SIZE
     * LOGICAL_MEMORY_MAX_SIZE
     */
    private static void addLogicalMemory(RowDataPacket row) {
        MemoryPool globalMemoryPool = MemoryManager.getInstance().getGlobalMemoryPool();
        long limit = globalMemoryPool.getMaxLimit();
        if (limit == MemorySetting.UNLIMITED_SIZE) {
            // -1 represents unlimited
            limit = -1;
        }
        row.add(LongUtil.toBytes(globalMemoryPool.getMemoryUsage()));
        row.add(LongUtil.toBytes(limit));
    }

    /**
     * cache directory on disk
     * FILESYSTEM_USED_SIZE_ON_DISK
     * FILESYSTEM_MAX_SIZE_ON_DISK
     */
    private static void addFileSystemOnDisk(RowDataPacket row, CacheManager cacheManager) {
        if (cacheManager instanceof FileMergeCacheManager) {
            FileMergeCacheManager fmCacheManager = (FileMergeCacheManager) cacheManager;
            row.add(LongUtil.toBytes(
                fmCacheManager.calcCacheSize().longValue()));
            row.add(LongUtil.toBytes(
                FileConfig.getInstance().getMergeCacheConfig().getMaxInDiskCacheSize().toBytes()));
            return;
        }

        row.add(null);
        row.add(null);
    }

    /**
     * cache in memory, size in bytes
     * FILESYSTEM_CACHE_USED_SIZE
     * FILESYSTEM_CACHE_MAX_SIZE
     */
    private static void addFileSystemInMemory(RowDataPacket row, CacheManager cacheManager) {
        if (cacheManager instanceof FileMergeCacheManager) {
            FileMergeCacheManager fmCacheManager = (FileMergeCacheManager) cacheManager;
            row.add(LongUtil.toBytes(
                fmCacheManager.getStats().getInMemoryRetainedBytes()));
            row.add(LongUtil.toBytes(
                FileConfig.getInstance().getMergeCacheConfig().getMaxInMemoryCacheSize().toBytes()));
            return;
        }

        row.add(null);
        row.add(null);
    }

    /**
     * BLOCK_CACHE_USED_SIZE
     * BLOCK_CACHE_MAX_SIZE
     */
    private static void addBlockCache(RowDataPacket row) {
        BlockCacheManager<Block> blockCacheManager = BlockCacheManager.getInstance();
        row.add(LongUtil.toBytes(blockCacheManager.getMemorySize()));
        row.add(LongUtil.toBytes(BlockCacheManager.MAXIMUM_MEMORY_SIZE));
    }

    /**
     * VERSION_CACHE_USED_SIZE
     * VERSION_CACHE_MAX_SIZE
     */
    private static void addVersionCache(RowDataPacket row, FileVersionStorage fileVersionStorage) {
        row.add(LongUtil.toBytes(fileVersionStorage.getUsedCacheSize()));
        row.add(LongUtil.toBytes(fileVersionStorage.getMaxCacheSize()));
    }

    /**
     * BLOCK_CACHE_HIT_RATE
     */
    private static void addBlockHitRate(RowDataPacket row, String charset) {
        BlockCacheManager<Block> blockCacheManager = BlockCacheManager.getInstance();
        long hitCount = blockCacheManager.getHitCount();
        long missCount = blockCacheManager.getMissCount();
        row.add(StringUtil.encode(getHitRate(hitCount, hitCount + missCount), charset));
    }

    /**
     * VERSION_CACHE_HIT_RATE
     */
    private static void addVersionHitRate(RowDataPacket row, String charset, FileVersionStorage fileVersionStorage) {
        long hitCount = fileVersionStorage.getHitCount();
        long missCount = fileVersionStorage.getMissCount();
        row.add(StringUtil.encode(getHitRate(hitCount, hitCount + missCount), charset));
    }

    /**
     * FILESYSTEM_CACHE_HIT_RATE
     */
    private static void addFsHitRate(RowDataPacket row, String charset, CacheManager ossCacheManager) {
        if (ossCacheManager != null) {
            CacheStats cacheStats = ((FileMergeCacheManager) ossCacheManager).getStats();
            long hitCount = cacheStats.getCacheHit();
            long missCount = cacheStats.getCacheMiss();
            row.add(StringUtil.encode(getHitRate(hitCount, hitCount + missCount), charset));
            return;
        }

        row.add(null);
    }

    private static String getHitRate(long hitCount, long totalCount) {
        if (totalCount <= 0) {
            return String.format("%.2f", 0D);
        }
        return String.format("%.2f", (hitCount * 100F) / (totalCount));
    }

    /**
     * INCREMENT_OPEN_FD
     * SNAPSHOT_OPEN_FD
     */
    private static void addFileDescriptor(RowDataPacket row, FileVersionStorage fileVersionStorage) {
        row.add(LongUtil.toBytes(fileVersionStorage.getOpenedIncrementFileCount()));
        row.add(LongUtil.toBytes(getSnapshotFd()));
    }

    /**
     * LOADED_VERSION_FILES
     * LOADED_VERSION_NUM
     */
    private static void addLoadedVersion(RowDataPacket row) {
        DynamicColumnarManager columnarManager = DynamicColumnarManager.getInstance();
        row.add(LongUtil.toBytes(columnarManager.getLoadedAppendFileCount()));
        row.add(LongUtil.toBytes(columnarManager.getLoadedVersionCount()));
    }

    /**
     * INCREMENT_FILE_REQUEST
     * SNAPSHOT_FILE_REQUEST
     */
    private static void addFileReq(RowDataPacket row) {
        DynamicColumnarManager columnarManager = DynamicColumnarManager.getInstance();
        row.add(LongUtil.toBytes(columnarManager.getAppendFileAccessCount()));
        row.add(LongUtil.toBytes(ColumnarScanExec.getSnapshotFileAccessCount()));
    }

    /**
     * OSS_READ_REQUEST
     * OSS_READ_SIZE
     */
    private static void addOssRead(RowDataPacket row, OSSFileSystem ossFileSystem) {
        if (ossFileSystem != null) {
            FileSystem.Statistics statistics = ossFileSystem.getStore().getStatistics();
            if (statistics != null) {
                row.add(LongUtil.toBytes(statistics.getReadOps()));
                row.add(LongUtil.toBytes(statistics.getBytesRead()));
                return;
            }
        }

        // oss statistics does not exist
        row.add(null);
        row.add(null);
    }

    /**
     * IO_THREAD_QUEUE
     * SCAN_THREAD_QUEUE
     */
    private static void addColumnarScanPool(RowDataPacket row) {
        ExecutorService ioExecutor = ColumnarScanExec.getIoExecutor();
        if (ioExecutor instanceof ThreadPoolExecutor) {
            long queueSize = ((ThreadPoolExecutor) ioExecutor).getQueue().size();
            row.add(LongUtil.toBytes(queueSize));
        } else {
            row.add(null);
        }

        ExecutorService scanExecutor = ColumnarScanExec.getScanExecutor();
        if (scanExecutor instanceof ThreadPoolExecutor) {
            long queueSize = ((ThreadPoolExecutor) scanExecutor).getQueue().size();
            row.add(LongUtil.toBytes(queueSize));
        } else {
            row.add(null);
        }
    }

    /**
     * TP_EXEC_QUEUE
     * AP_EXEC_QUEUE
     * EXEC_BLOCK_QUEUE
     */
    private static void addExecPool(RowDataPacket row) {
        if (ServiceProvider.getInstance().getServer() != null) {
            TaskExecutor priorityExecutor = ServiceProvider.getInstance().getServer().getTaskExecutor();
            PriorityExecutorInfo tpInfo = priorityExecutor.getHighPriorityInfo();
            PriorityExecutorInfo apInfo = priorityExecutor.getLowPriorityInfo();

            row.add(LongUtil.toBytes(tpInfo.getActiveCount()));
            row.add(LongUtil.toBytes(apInfo.getActiveCount()));
            row.add(LongUtil.toBytes(tpInfo.getBlockedSplitSize() + apInfo.getBlockedSplitSize()));
            return;
        }
        // fill default values
        row.add(LongUtil.toBytes(0));
        row.add(LongUtil.toBytes(0));
        row.add(LongUtil.toBytes(0));
    }

    private static long getSnapshotFd() {
        return 0;
    }

    private static long getIncrementFd() {
        return 0;
    }

    private static CacheManager getOssCacheManager() {
        FileSystemGroup group = FileSystemManager.getFileSystemGroup(Engine.OSS, false);
        if (group != null) {
            FileSystem masterFs = group.getMaster();
            if (masterFs instanceof FileMergeCachingFileSystem) {
                return ((FileMergeCachingFileSystem) masterFs).getCacheManager();
            }
        }
        return null;
    }

    private static OSSFileSystem getOssFileSystem() {
        FileSystemGroup group = FileSystemManager.getFileSystemGroup(Engine.OSS, false);
        if (group != null) {
            FileSystem masterFs = group.getMaster();
            if (masterFs instanceof OSSFileSystem) {
                return (OSSFileSystem) masterFs;
            } else if (masterFs instanceof CachingFileSystem) {
                FileSystem dataFileSystem = ((CachingFileSystem) masterFs).getDataTier();
                if (dataFileSystem instanceof OSSFileSystem) {
                    return (OSSFileSystem) dataFileSystem;
                }
            }
        }
        return null;
    }
}
