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
import com.alibaba.polardbx.executor.columnar.DeletionFileReader;
import com.alibaba.polardbx.executor.columnar.SimpleDeletionFileReader;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Set of TSOs that have been loaded for each partition of .del file
 */
public class MultiVersionDelPartitionInfo implements Purgeable {
    private final SortedSet<Long> readDelTso = new ConcurrentSkipListSet<>();
    private final Lock lock = new ReentrantLock();

    // there may be more than one deletion bitmap in a partition,
    // so we need to cache the read position of each bitmap
    // <delFileName, position>
    private final Map<String, Long> positionMap = new HashMap<>();

    public Long getLastTso() {
        if (readDelTso.isEmpty()) {
            return Long.MIN_VALUE;
        } else {
            return readDelTso.last();
        }
    }

    public void loadUntilTso(String logicalSchema, String logicalTable, String partitionName, long minTso, long tso,
                             BiConsumer<DeletionFileReader, ColumnarAppendedFilesRecord> delFileConsumer) {
        if (!readDelTso.isEmpty() && readDelTso.last() >= tso) {
            return;
        }

        long lastTso = readDelTso.isEmpty() ? Long.MIN_VALUE : readDelTso.last();

        List<String> minSnapshotDelFiles = ((DynamicColumnarManager) ColumnarManager.getInstance())
            .delFileNames(minTso, logicalSchema, logicalTable, partitionName);

        Map<String, List<ColumnarAppendedFilesRecord>> recordsForEachDelFile = new HashMap<>();
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarAppendedFilesAccessor columnarAppendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            columnarAppendedFilesAccessor.setConnection(connection);

            if (lastTso < minTso) {
                for (String delFileName : minSnapshotDelFiles) {
                    List<ColumnarAppendedFilesRecord> records = columnarAppendedFilesAccessor
                        .queryByFileNameAndMaxTso(delFileName, minTso);
                    if (records == null || records.isEmpty()) {
                        continue;
                    }

                    Preconditions.checkArgument(records.size() == 1);
                    ColumnarAppendedFilesRecord delRecord = records.get(0);
                    long pos = delRecord.appendOffset + delRecord.appendLength;

                    if (positionMap.getOrDefault(delFileName, 0L) >= pos) {
                        continue;
                    }

                    recordsForEachDelFile.computeIfAbsent(delFileName, s -> new ArrayList<>());
                    recordsForEachDelFile.get(delFileName).add(delRecord);
                }
            } else {
                minTso = lastTso;
            }

            if (minTso < tso) {
                columnarAppendedFilesAccessor.queryDelByPartitionBetweenTso(
                    logicalSchema,
                    logicalTable,
                    partitionName,
                    minTso,
                    tso
                ).forEach(record -> {
                    recordsForEachDelFile.computeIfAbsent(record.fileName, s -> new ArrayList<>());
                    recordsForEachDelFile.get(record.fileName).add(record);
                });
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to generate columnar snapshot of tso: %d", tso));
        }

        List<List<ColumnarAppendedFilesRecord>> sortedDelFileRecords =
            recordsForEachDelFile.values().stream()
                /* sorted by tso to make sure that the bitmap is applied in order */
                .sorted(Comparator.comparingLong(records -> records.get(0).checkpointTso))
                .collect(Collectors.toList());

        for (List<ColumnarAppendedFilesRecord> records : sortedDelFileRecords) {
            ColumnarAppendedFilesRecord firstRecord = records.get(0);
            ColumnarAppendedFilesRecord lastRecord = records.get(records.size() - 1);

            Engine engine = Engine.of(firstRecord.engine);
            String fileName = firstRecord.fileName;
            long maxReadPosition = lastRecord.appendOffset + lastRecord.appendLength;
            long lastEndPosition = positionMap.getOrDefault(fileName, 0L);

            if (lastEndPosition >= maxReadPosition) {
                continue;
            }

            try (SimpleDeletionFileReader fileReader = new SimpleDeletionFileReader()) {
                try {
                    fileReader.open(
                        engine,
                        fileName,
                        (int) lastEndPosition,
                        (int) (maxReadPosition - lastEndPosition)
                    );
                } catch (IOException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_LOAD_DEL_FILE, e,
                        String.format("Failed to open delete bitmap file, filename: %s, offset: %d, length: %d",
                            fileName, lastEndPosition, maxReadPosition - lastEndPosition));
                }

                for (ColumnarAppendedFilesRecord record : records) {
                    if (lastEndPosition >= record.appendOffset + record.appendLength) {
                        continue;
                    }

                    delFileConsumer.accept(fileReader, record);
                    readDelTso.add(record.checkpointTso);
                }
            } catch (Throwable t) {
                throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, t,
                    String.format("Failed to generate columnar snapshot of tso: %d", tso));
            }

            positionMap.put(fileName, maxReadPosition);
        }

        if (!readDelTso.isEmpty() && readDelTso.last() < tso) {
            readDelTso.add(tso);
        }

        // In case there are no .del files
        if (readDelTso.isEmpty()) {
            readDelTso.add(tso);
        }
    }

    public Lock getLock() {
        return lock;
    }

    public void purge(long tso) {
        lock.lock();
        try {
            // must keep one position info at least
            readDelTso.headSet(Long.min(tso + 1, readDelTso.last())).clear();
        } finally {
            lock.unlock();
        }
    }
}
