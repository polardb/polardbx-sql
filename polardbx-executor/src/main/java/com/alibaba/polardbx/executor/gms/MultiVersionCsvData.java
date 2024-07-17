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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.columnar.CSVFileReader;
import com.alibaba.polardbx.executor.columnar.RawOrcTypeCsvReader;
import com.alibaba.polardbx.executor.columnar.SimpleCSVFileReader;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MultiVersionCsvData implements Purgeable {
    private static final Logger LOGGER = LoggerFactory.getLogger("COLUMNAR_TRANS");

    protected final String csvFileName;
    // tso - <end position, cache>
    private SortedMap<Long, Pair<Long, List<Chunk>>> allChunks = new ConcurrentSkipListMap<>();
    private final AtomicLong openedFileCount;
    private final Lock lock = new ReentrantLock();

    public MultiVersionCsvData(String csvFileName, AtomicLong openedFileCount) {
        this.csvFileName = csvFileName;
        this.openedFileCount = openedFileCount;
    }

    @Nullable
    public SortedMap<Long, Pair<Long, List<Chunk>>> getChunksWithTso(long tso) {
        // current tso is not loaded, should read from files
        if (allChunks.isEmpty() || allChunks.lastKey() < tso) {
            return null;
        }
        return allChunks.headMap(tso + 1);
    }

    private List<ColumnarAppendedFilesRecord> loadDeltaStateFromGms(long minTso, long latestTso, long tso) {
        List<ColumnarAppendedFilesRecord> appendedFilesRecords = new ArrayList<>();

        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarAppendedFilesAccessor columnarAppendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            columnarAppendedFilesAccessor.setConnection(connection);

            // single huge part below minTso
            if (latestTso < minTso) {
                appendedFilesRecords.addAll(
                    columnarAppendedFilesAccessor.queryLatestByFileNameBetweenTso(csvFileName, latestTso, minTso));
            } else {
                minTso = latestTso;
            }

            if (minTso < tso) {
                // multi-version parts
                appendedFilesRecords.addAll(
                    columnarAppendedFilesAccessor.queryByFileNameBetweenTso(csvFileName, minTso, tso));
            }

        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to generate columnar snapshot of tso: %d", tso));
        }
        return appendedFilesRecords;
    }

    public void loadUntilTso(long minTso, long tso) {
        if (!allChunks.isEmpty() && allChunks.lastKey() >= tso) {
            return;
        }

        minTso = Math.min(minTso, tso);

        // The latest tso which has already been loaded by version chain
        long latestTso = allChunks.isEmpty() ? Long.MIN_VALUE : allChunks.lastKey();

        long lastEndPosition = latestTso == Long.MIN_VALUE ? 0 : allChunks.get(latestTso).getKey();

        List<ColumnarAppendedFilesRecord> appendedFilesRecords = loadDeltaStateFromGms(minTso, latestTso, tso);

        if (!appendedFilesRecords.isEmpty()) {
            ColumnarAppendedFilesRecord lastRecord = appendedFilesRecords.get(appendedFilesRecords.size() - 1);

            FileMeta fileMeta = ColumnarManager.getInstance().fileMetaOf(csvFileName);
            Engine engine = fileMeta.getEngine();
            List<ColumnMeta> columnMetas = fileMeta.getColumnMetas();

            long maxReadPosition = lastRecord.appendOffset + lastRecord.appendLength;
            openedFileCount.incrementAndGet();
            try (SimpleCSVFileReader csvFileReader = new SimpleCSVFileReader()) {
                csvFileReader.open(new ExecutionContext(), columnMetas, FileVersionStorage.CSV_CHUNK_LIMIT, engine,
                    csvFileName,
                    (int) lastEndPosition,
                    (int) (maxReadPosition - lastEndPosition));
                for (ColumnarAppendedFilesRecord record : appendedFilesRecords) {
                    long newEndPosition = record.appendOffset + record.appendLength;

                    List<Chunk> results = new ArrayList<>();
                    // It may cause OOM
                    Chunk result;
                    while ((result = csvFileReader.nextUntilPosition(newEndPosition)) != null) {
                        results.add(result);
                    }

                    allChunks.put(record.checkpointTso, Pair.of(newEndPosition, results));
                }
            } catch (Throwable t) {
                throw new TddlRuntimeException(ErrorCode.ERR_LOAD_CSV_FILE, t,
                    String.format("Failed to load read csv file, file name: %s, last tso: %d, snapshot tso: %d",
                        csvFileName, latestTso, tso));
            } finally {
                openedFileCount.decrementAndGet();
            }
        } else {
            // add a new version to bump the tso when the file is not appended
            // preventing access GMS over and over again when there is no newer version
            allChunks.put(tso, Pair.of(lastEndPosition, Collections.emptyList()));
        }
    }

    /**
     * Never use any cache.
     */
    public static List<Chunk> loadRawOrcTypeUntilTso(long tso,
                                                     AtomicLong openedFileCount,
                                                     String csvFileName,
                                                     ExecutionContext context) {
        // The latest tso which has already been loaded by version chain
        long latestTso = Long.MIN_VALUE;

        ColumnarAppendedFilesRecord record;

        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarAppendedFilesAccessor columnarAppendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            columnarAppendedFilesAccessor.setConnection(connection);
            record = columnarAppendedFilesAccessor
                .queryLatestByFileNameBetweenTso(csvFileName, latestTso, tso).get(0);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("[RawOrcType]Failed to generate columnar snapshot of tso: %d", tso));
        }

        if (null != record) {
            FileMeta fileMeta = ColumnarManager.getInstance().fileMetaOf(csvFileName);
            Engine engine = fileMeta.getEngine();
            List<ColumnMeta> columnMetas = fileMeta.getColumnMetas();

            long maxReadPosition = record.appendOffset + record.appendLength;
            openedFileCount.incrementAndGet();
            try (CSVFileReader csvFileReader = new RawOrcTypeCsvReader()) {
                csvFileReader.open(context, columnMetas, FileVersionStorage.CSV_CHUNK_LIMIT, engine,
                    csvFileName,
                    0,
                    (int) maxReadPosition);
                List<Chunk> results = new ArrayList<>();
                // It may cause OOM
                Chunk result;
                while ((result = csvFileReader.nextUntilPosition(maxReadPosition)) != null) {
                    results.add(result);
                }
                return results;
            } catch (Throwable t) {
                throw new TddlRuntimeException(ErrorCode.ERR_LOAD_CSV_FILE, t,
                    String.format("[RawOrcType]Failed to load read csv file, "
                            + "file name: %s, last tso: %d, snapshot tso: %d",
                        csvFileName, latestTso, tso));
            } finally {
                openedFileCount.decrementAndGet();
            }
        }
        return null;
    }

    public Lock getLock() {
        return lock;
    }

    public void purge(long tso) {
        // Merge data in memory is not economical, so we reload data from file for small part of chunk
        if (allChunks.isEmpty() || allChunks.firstKey() >= tso) {
            return;
        }

        // for csv whose new version have not been loaded, we skip the purge
        if (allChunks.lastKey() < tso) {
            return;
        }

        long lastPurgeTso = allChunks.firstKey();
        long floorTso = allChunks.headMap(tso + 1).lastKey();

        if (lastPurgeTso == floorTso) {
            return;
        }

        Pair<Long, List<Chunk>> firstEntry = allChunks.get(lastPurgeTso);
        long firstPos = firstEntry.getKey();
        List<Chunk> firstChunk = firstEntry.getValue();
        long purgePos = allChunks.get(floorTso).getKey();

        List<Chunk> purgedChunkList = new ArrayList<>();
        List<Chunk> orphanChunkList = new ArrayList<>();
        // If first chunk is less than 1000 lines, consider purging together.
        // for extremely large row, this strategy may perform bad
        if (firstChunk.size() == 1 && firstChunk.get(0).getPositionCount() < FileVersionStorage.CSV_CHUNK_LIMIT) {
            firstPos = 0;
        } else {
            purgedChunkList.addAll(firstChunk);
        }

        long currentPos = firstPos;
        openedFileCount.incrementAndGet();
        try (SimpleCSVFileReader csvFileReader = new SimpleCSVFileReader()) {
            FileMeta fileMeta = ColumnarManager.getInstance().fileMetaOf(csvFileName);
            Engine engine = fileMeta.getEngine();
            List<ColumnMeta> columnMetas = fileMeta.getColumnMetas();

            csvFileReader.open(new ExecutionContext(), columnMetas, FileVersionStorage.CSV_CHUNK_LIMIT, engine,
                csvFileName,
                (int) firstPos,
                (int) (purgePos - firstPos));
            Chunk result;
            while ((result = csvFileReader.next()) != null) {
                if (result.getPositionCount() >= FileVersionStorage.CSV_CHUNK_LIMIT) {
                    purgedChunkList.add(result);
                    currentPos = csvFileReader.position();
                } else {
                    // not reach 1000 lines, could be purged next time
                    orphanChunkList.add(result);
                }
            }
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_CSV_FILE, t,
                String.format(
                    "Failed to load read csv file while purging, file name: %s, last tso: %d, purge tso: %d",
                    csvFileName, lastPurgeTso, tso));
        } finally {
            openedFileCount.decrementAndGet();
        }

        SortedMap<Long, Pair<Long, List<Chunk>>> newChunks = new ConcurrentSkipListMap<>();
        if (!orphanChunkList.isEmpty()) {
            newChunks.put(tso - 1, new Pair<>(currentPos, purgedChunkList));
            newChunks.put(tso, new Pair<>(purgePos, orphanChunkList));
        } else {
            newChunks.put(tso, new Pair<>(currentPos, purgedChunkList));
        }

        // minimize the lock:
        // since the version chain is append-only, loadUtilTso() will not affect version before purge tso
        // so only the operations with larger tso require lock
        lock.lock();
        try {
            newChunks.putAll(allChunks.tailMap(tso + 1));
            // hot swap the cache
            LOGGER.debug(
                String.format("Csv purge finished: fileName: %s, versions before purge: %d, after purge: %d",
                    csvFileName, this.allChunks.size(), newChunks.size()));
            this.allChunks = newChunks;
        } finally {
            lock.unlock();
        }
    }
}
