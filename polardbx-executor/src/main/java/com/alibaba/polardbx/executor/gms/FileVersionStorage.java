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
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.columnar.DeletionFileReader;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.roaringbitmap.RoaringBitmap;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

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
    private final AtomicLong csvCacheSizeInBytes = new AtomicLong(0);
    private final AtomicLong delCacheSizeInBytes = new AtomicLong(0);
    private volatile int csvCacheSize = DynamicConfig.getInstance().getCsvCacheSize();

    public FileVersionStorage(DynamicColumnarManager columnarManager) {
        this.columnarManager = columnarManager;
    }

    public void open() {
        // TODO(siyun): memory management

        this.csvDataMap = Caffeine.newBuilder()
            .maximumSize(csvCacheSize)
            .removalListener((key, value, cause) -> {
                if (value != null) {
                    csvCacheSizeInBytes.addAndGet(
                    -((MultiVersionCsvData) value).getCurrentMemoryUsed());
                }
            })
            // TODO(siyun): support spill to disk while being evicted
            .build(key -> new MultiVersionCsvData(key, openedIncrementFileCount, csvCacheSizeInBytes));

        this.delDataMap = Caffeine.newBuilder()
            // The deletion bitmap could NOT be directly invalidated, which may lead to inconsistency
            // This cache can be invalidated by purge if the whole file is invisible at minTso
            .removalListener((key, value, cause) -> {
                if (value != null) {
                    delCacheSizeInBytes.addAndGet(
                    -((MultiVersionDelData) value).getCurrentMemoryUsed());
                }
            })
            .build(key -> new MultiVersionDelData(delCacheSizeInBytes));

        this.delDataTsoMap = Caffeine.newBuilder()
            // This cache can only be invalidated if the CCI is removed
            .build((key -> new MultiVersionDelPartitionInfo()));
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

    public boolean isCsvPositionLoaded(String csvFileName, long tso) {
        MultiVersionCsvData data;
        data = csvDataMap.get(csvFileName);
        if (data == null) {
            csvDataMap.invalidate(csvFileName);
            return false;
        } else {
            return data.isCsvPositionLoaded(tso);
        }
    }

    /**
     * @param csvCheckpointTso this tso must be taken from columnar_appended_files
     */
    public List<Chunk> csvData(long csvCheckpointTso, long readTso, String csvFileName) {
        MultiVersionCsvData data;

        data = csvDataMap.get(csvFileName);
        if (data == null) {
            csvDataMap.invalidate(csvFileName);
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_CSV_FILE,
                String.format("Failed to load csv file, filename: %s, tso: %d", csvFileName, readTso));
        }

        // find csv cache whose tso <= given tso
        SortedMap<Long, Pair<Long, List<Chunk>>> chunkSortedMap = data.getChunksWithTso(csvCheckpointTso, readTso);

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
            data.loadUntilTso(columnarManager.getMinTso(), csvCheckpointTso);

            return Objects.requireNonNull(data.getChunksWithTso(csvCheckpointTso, readTso)).values().stream()
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
            RoaringBitmap bitmap = Objects.requireNonNull(delDataMap.get(fileName)).buildDeleteBitMap(tso);

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
        } catch (NullPointerException e) {
            throw GeneralUtil.nestedException(e);
        }

    }

    // Used for old columnar table scan
    @Deprecated
    public int fillSelection(String fileName, long tso, int[] selection, LongBlock positionBlock) {
        try {
            RoaringBitmap bitmap = Objects.requireNonNull(delDataMap.get(fileName)).buildDeleteBitMap(tso);

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
        } catch (NullPointerException e) {
            throw GeneralUtil.nestedException(e);
        }

    }

    private RoaringBitmap buildDeleteBitMap(String fileName, long tso) {
        try {
            return Objects.requireNonNull(delDataMap.get(fileName)).buildDeleteBitMap(tso);
        } catch (NullPointerException e) {
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
                        Objects.requireNonNull(delDataMap.get(deletedFileName)).putNewTsoBitMap(delTso, bitmap);
                    } catch (NullPointerException e) {
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
        delInfo = this.delDataTsoMap.get(partitionId);
        if (delInfo == null) {
            this.delDataTsoMap.invalidate(partitionId);
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_DEL_FILE,
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

    public synchronized void resetCsvCacheSize(int newCacheSize) {
        if (csvCacheSize != newCacheSize && newCacheSize > 0) {
            csvCacheSize = newCacheSize;
            csvDataMap.policy().eviction().ifPresent(eviction -> eviction.setMaximum(newCacheSize));
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
            total += csvDataMap.estimatedSize();
        }
        if (delDataMap != null) {
            total += delDataMap.estimatedSize();
        }
        return total;
    }

    public long getUsedCacheSize() {
        long total = 0;
        if (csvDataMap != null) {
            total += csvDataMap.estimatedSize();
        }
        if (delDataMap != null) {
            total += delDataMap.estimatedSize();
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

    public long getCsvCacheSizeInBytes() {
        return csvCacheSizeInBytes.get();
    }

    public long getDelCacheSizeInBytes() {
        return delCacheSizeInBytes.get();
    }
}
