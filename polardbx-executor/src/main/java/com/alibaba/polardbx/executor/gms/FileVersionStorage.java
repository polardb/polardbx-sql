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

package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.columnar.DeletionFileReader;
import com.alibaba.polardbx.executor.operator.spill.MemorySpillerFactory;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.spill.SpillSpaceManager;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.RoaringBitmap;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.gms.DynamicColumnarManager.MAXIMUM_FILE_META_COUNT;

/**
 * Version management of columnar store.
 * 1. Update the delta-state according to columnar-appended-files record with give tso in a lazy way.
 * 2. Maintain the multi-version csv/del data. (build/purge/read)
 */
public class FileVersionStorage implements Closeable, Purgeable {
    private static final Logger LOGGER = LoggerFactory.getLogger("COLUMNAR_TRANS");

    public static final int CSV_CHUNK_LIMIT = 1000;

    private final DynamicColumnarManager columnarManager;

    /**
     * Cache the csv cached chunks of all csv files.
     * This cache is HEAVY
     */
    private LoadingCache<String, MultiVersionCsvData> csvDataMap;

    /**
     * Cache the bitmaps for all table files in this schema.
     * This cache is HEAVY
     */
    private LoadingCache<String, MultiVersionDelData> delDataMap;

    /**
     * Maintain the already loaded tso for each partition
     * TODO(siyun): for multi-version partition info, this map is MIXED with different versions, which should be separated for SPLIT PARTITION
     */
    private LoadingCache<PartitionId, MultiVersionDelPartitionInfo> delDataTsoMap;

    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);
    private final AtomicLong openedIncrementFileCount = new AtomicLong(0);

    public FileVersionStorage(DynamicColumnarManager columnarManager) {
        this.columnarManager = columnarManager;
    }

    public void open() {
        // TODO(siyun): memory management

        this.csvDataMap = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_FILE_META_COUNT)
            // TODO(siyun): support spill to disk while being evicted
            .build(new CacheLoader<String, MultiVersionCsvData>() {
                @Override
                public MultiVersionCsvData load(@NotNull String key) {
                    return new MultiVersionCsvData(key, openedIncrementFileCount);
                }
            });

        this.delDataMap = CacheBuilder.newBuilder()
            // The deletion bitmap could NOT be directly invalidated, which may lead to inconsistency
            // This cache can be invalidated by purge if the whole file is invisible at minTso
            .build(new CacheLoader<String, MultiVersionDelData>() {
                @Override
                public MultiVersionDelData load(@NotNull String key) {
                    return new MultiVersionDelData();
                }
            });

        this.delDataTsoMap = CacheBuilder.newBuilder()
            // This cache can only be invalidated if the CCI is removed
            .build((new CacheLoader<PartitionId, MultiVersionDelPartitionInfo>() {
                @Override
                public MultiVersionDelPartitionInfo load(@NotNull PartitionId key) throws Exception {
                    return new MultiVersionDelPartitionInfo();
                }
            }));
    }

    public void purge(long tso, String logicalTable, String partName) {

    }

    /**
     * Purge all cache below tso
     */
    public void purge(long tso) {
        for (MultiVersionDelPartitionInfo delPartitionInfo : delDataTsoMap.asMap().values()) {
            delPartitionInfo.purge(tso);
        }

        for (MultiVersionDelData delData : delDataMap.asMap().values()) {
            delData.purge(tso);
        }

        for (MultiVersionCsvData csvData : csvDataMap.asMap().values()) {
            csvData.purge(tso);
        }
    }

    public void purgeByFile(String fileName) {
        delDataMap.invalidate(fileName);
        csvDataMap.invalidate(fileName);
    }

    /**
     * @param tso this tso must be taken from columnar_appended_files
     */
    public List<Chunk> csvData(long tso, String csvFileName) {
        MultiVersionCsvData data;

        try {
            data = csvDataMap.get(csvFileName);
        } catch (ExecutionException e) {
            csvDataMap.invalidate(csvFileName);
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_CSV_FILE, e.getCause(),
                String.format("Failed to load csv file, filename: %s, tso: %d", csvFileName, tso));
        }

        // find csv cache whose tso <= given tso
        SortedMap<Long, Pair<Long, List<Chunk>>> chunkSortedMap = data.getChunksWithTso(tso);

        if (chunkSortedMap != null && !chunkSortedMap.isEmpty()) {
            hitCount.getAndIncrement();
            return chunkSortedMap.values().stream()
                .flatMap(part -> part.getValue().stream()).collect(Collectors.toList());
        }

        // Case: csv cache missed
        missCount.getAndIncrement();
        Lock writeLock = data.getLock();
        writeLock.lock();
        try {
            data.loadUntilTso(columnarManager.getMinTso(), tso);

            return Objects.requireNonNull(data.getChunksWithTso(tso)).values().stream()
                .flatMap(part -> part.getValue().stream()).collect(Collectors.toList());
        } finally {
            writeLock.unlock();
        }
    }

    // Used for old columnar table scan
    @Deprecated
    public int fillSelection(String fileName, long tso, int[] selection, LongColumnVector longColumnVector,
                             int batchSize) {
        try {
            RoaringBitmap bitmap = delDataMap.get(fileName).buildDeleteBitMap(tso);

            int selSize = 0;
            for (int index = 0;
                 index < batchSize && selSize < selection.length;
                 index++) {
                // for each position value in position-block
                // check all bitmaps in sorted-map
                if (longColumnVector.isNull[index]) {
                    throw GeneralUtil.nestedException("The position vector cannot be null");
                }
                int position = (int) longColumnVector.vector[index];

                if (!bitmap.contains(position)) {
                    selection[selSize++] = index;
                }
            }

            return selSize;
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }

    }

    // Used for old columnar table scan
    @Deprecated
    public int fillSelection(String fileName, long tso, int[] selection, LongBlock positionBlock) {
        try {
            RoaringBitmap bitmap = delDataMap.get(fileName).buildDeleteBitMap(tso);

            int selSize = 0;
            for (int index = 0;
                 index < positionBlock.getPositionCount() && selSize < selection.length;
                 index++) {
                // for each position value in position-block
                // check all bitmaps in sorted-map
                int position = positionBlock.getInt(index);

                if (!bitmap.contains(position)) {
                    selection[selSize++] = index;
                }
            }

            return selSize;
        } catch (ExecutionException e) {
            throw GeneralUtil.nestedException(e);
        }

    }

    private RoaringBitmap buildDeleteBitMap(String fileName, long tso) {
        try {
            return delDataMap.get(fileName).buildDeleteBitMap(tso);
        } catch (ExecutionException e) {
            delDataMap.invalidate(fileName);
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_DEL_FILE, e.getCause(),
                String.format("Failed to load delete bitmap of certain file , file name: %s, tso: %d", fileName, tso));
        }
    }

    protected void loadDeleteBitMapFromFile(DeletionFileReader fileReader, ColumnarAppendedFilesRecord record) {
        // Deserialize progress.
        DeletionFileReader.DeletionEntry entry;
        int endPosition = (int) (record.appendOffset + record.appendLength);
        while (fileReader.position() < endPosition && (entry = fileReader.next()) != null) {
            final int fileId = entry.getFileId();
            final long delTso = entry.getTso();
            final RoaringBitmap bitmap = entry.getBitmap();
            columnarManager.fileNameOf(
                    record.logicalSchema, Long.parseLong(record.logicalTable), record.partName, fileId)
                .ifPresent(deletedFileName -> {
                    try {
                        delDataMap.get(deletedFileName).putNewTsoBitMap(delTso, bitmap);
                    } catch (ExecutionException e) {
                        throw new TddlRuntimeException(ErrorCode.ERR_LOAD_DEL_FILE, e,
                            String.format("Failed to load delete bitmap file, filename: %s, fileId: %d, tso: %d",
                                record.fileName, fileId, delTso));
                    }
                });
        }
    }

    public RoaringBitmap getDeleteBitMap(FileMeta fileMeta, long tso) {
        MultiVersionDelPartitionInfo delInfo;
        PartitionId partitionId = PartitionId.of(
            fileMeta.getPartitionName(),
            fileMeta.getLogicalTableSchema(),
            Long.valueOf(fileMeta.getLogicalTableName()));
        try {
            delInfo = this.delDataTsoMap.get(partitionId);
        } catch (ExecutionException e) {
            this.delDataTsoMap.invalidate(partitionId);
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_DEL_FILE, e.getCause(),
                String.format(
                    "Failed to load delete bitmap of certain partition, partition name: %s, schema name: %s, table name: %s, tso: %d",
                    partitionId.getPartName(), partitionId.getLogicalSchema(), partitionId.getTableId(), tso));
        }

        // cache hit
        if (delInfo.getLastTso() >= tso) {
            hitCount.getAndIncrement();
            return buildDeleteBitMap(fileMeta.getFileName(), tso);
        }

        // cache miss
        missCount.getAndIncrement();
        Lock writeLock = delInfo.getLock();
        writeLock.lock();

        try {
            delInfo.loadUntilTso(
                fileMeta.getLogicalTableSchema(),
                fileMeta.getLogicalTableName(),
                fileMeta.getPartitionName(),
                columnarManager.getMinTso(),
                tso,
                this::loadDeleteBitMapFromFile
            );

            return buildDeleteBitMap(fileMeta.getFileName(), tso);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void close() {
        try {

        } catch (Throwable t) {
            LOGGER.error("Fail to close the file version storage", t);
        }
    }

    private static final int COLUMNAR_VERSION_FIELDS = 11;

    public List<byte[][]> generatePacket() {
        // 1. tso
        // 2. schema
        // 3. table
        // 4. part
        // 5. csv-delta
        // 6. csv-offset
        // 7. csv-length
        // 8. del-delta
        // 9. del-offset
        // 10. del-length
        // 11. state
        // TODO(siyun): "SHOW COLUMNAR VERSION command"
        return new ArrayList<>();
    }

    /**
     * TODO record max size here
     */
    public long getMaxCacheSize() {
        long total = 0;
        if (csvDataMap != null) {
            total += csvDataMap.size();
        }
        if (delDataMap != null) {
            total += delDataMap.size();
        }
        return total;
    }

    public long getUsedCacheSize() {
        long total = 0;
        if (csvDataMap != null) {
            total += csvDataMap.size();
        }
        if (delDataMap != null) {
            total += delDataMap.size();
        }
        return total;
    }

    public long getHitCount() {
        return hitCount.get();
    }

    public long getMissCount() {
        return missCount.get();
    }

    public long getOpenedIncrementFileCount() {
        return openedIncrementFileCount.get();
    }
}
