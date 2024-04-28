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
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.split.SplitManagerImpl;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplified;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.Data;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Multi-version columnar snapshot of certain partition
 * Can provide a snapshot of certain tso
 */
public class MultiVersionColumnarSnapshot implements Purgeable {
    private static final Logger SPLIT_MANAGER_LOGGER = LoggerFactory.getLogger(SplitManagerImpl.class);

    final private String logicalSchema;
    final private Long tableId;

    /**
     * In the range of [minTso, latestTso], all files multi-version info of the partition have been loaded.
     */
    private final DynamicColumnarManager columnarManager;
    private volatile long latestTso = Long.MIN_VALUE;

    /**
     * part_name -> (file_name -> (commit_ts, remove_ts))
     */
    private final Map<String, Map<String, Pair<Long, Long>>> allPartsTsoInfo = new ConcurrentHashMap<>();

    /**
     * part_name -> latest_tso
     */
    private final Map<String, Long> latestTsoForEachPart = new ConcurrentHashMap<>();

    public MultiVersionColumnarSnapshot(DynamicColumnarManager columnarManager, String logicalSchema, Long tableId) {
        this.columnarManager = columnarManager;
        this.logicalSchema = logicalSchema;
        this.tableId = tableId;
    }

    /**
     * The snapshot of all visible orc files and csv files in given tso.
     */
    @Data
    public static class ColumnarSnapshot {
        private final List<String> orcFiles;
        private final List<String> csvFiles;
        private final List<String> delFiles;

        public ColumnarSnapshot() {
            this.orcFiles = new ArrayList<>();
            this.csvFiles = new ArrayList<>();
            this.delFiles = new ArrayList<>();
        }
    }

    /**
     * Fetch delta files which satisfy:
     * lastTso < commitTso <= tso AND minTso < removeTso
     * OR
     * lastTso < removeTso <= tso AND commitTso <= lastTso
     * *   01234567890123456789
     * F1: |-------|
     * F2:     |-------|
     * F3:                 |--------|
     * F4:           |-------------...(no removeTso)
     * F5:           |---|
     * F6:         |--|
     * *          7   11  15
     * *          |   |   |
     * *          | minTso|
     * *        latestTso |
     * *                 tso
     * In this case minTso = 9, lastTso = 7, tso = 11, we should fetch F1, F2, F4 and F5
     * After updating the tso info using these filesRecords, we can generate any snapshots in [minTso, tso]
     */
    private void loadUntilTso(long tso, long minTso) {
        List<FilesRecordSimplified> filesRecords = loadDeltaFilesInfoFromGms(latestTso, tso);

        for (FilesRecordSimplified fileRecord : filesRecords) {
            String fileName = fileRecord.fileName;
            String partName = fileRecord.partitionName;
            Long commitTs = fileRecord.commitTs;
            Long removeTs = fileRecord.removeTs;

            Map<String, Pair<Long, Long>> tsoInfo =
                allPartsTsoInfo.computeIfAbsent(partName, s -> new ConcurrentHashMap<>());

            long curLatestTso = removeTs != null ? removeTs : commitTs;

            latestTsoForEachPart.compute(partName, (p, latestTsoForThisPart) -> {
                if (latestTsoForThisPart == null) {
                    return curLatestTso;
                } else {
                    return Math.max(latestTsoForThisPart, curLatestTso);
                }
            });

            if (tsoInfo.containsKey(fileName)) {
                if (removeTs == null) {
                    continue;
                }

                if (removeTs <= minTso) {
                    tsoInfo.remove(fileName);
                    columnarManager.putPurgedFile(fileName);
                } else {
                    tsoInfo.put(fileName, Pair.of(commitTs, removeTs));
                }
            } else {
                if (removeTs == null || removeTs > minTso) {
                    tsoInfo.put(fileName, Pair.of(commitTs, removeTs));
                }
            }
        }
        latestTso = latestTsoForEachPart.values().stream().reduce(Long::min).orElse(Long.MIN_VALUE);
    }

    private List<FilesRecordSimplified> loadDeltaFilesInfoFromGms(long lastTso, long tso) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(connection);

            return filesAccessor
                .queryColumnarDeltaFilesByTsoAndTableId(tso, lastTso, logicalSchema, String.valueOf(tableId));
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT,
                String.format("Failed to generate columnar snapshot, tso: %d, schema: %s, tableId: %d",
                    tso, logicalSchema, tableId));
        }
    }

    public ColumnarSnapshot generateSnapshot(final String partitionName, final long tso) {
        long ioCost = 0L;
        long totalCost = 0L;
        long startTime = System.nanoTime();
        long latestTsoBackUp = latestTso;

        long minTso = columnarManager.getMinTso();
        if (tso < minTso) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT,
                String.format("Snapshot of tso[%d] has been purged!", tso));
        }

        if (latestTso < tso) {
            long startIOTime = System.nanoTime();
            synchronized (this) {
                // In case the tso has not been loaded
                // Assuming that the tso is reliable
                if (latestTso < tso) {
                    loadUntilTso(tso, minTso);
                }
            }
            ioCost = System.nanoTime() - startIOTime;
        }

        ColumnarSnapshot snapshot = new ColumnarSnapshot();
        Map<String, Pair<Long, Long>> tsoInfo =
            allPartsTsoInfo.computeIfAbsent(partitionName, s -> new ConcurrentHashMap<>());
        tsoInfo.forEach((fileName, commitAndRemoveTs) -> {
            Long commitTs = commitAndRemoveTs.getKey();
            Long removeTs = commitAndRemoveTs.getValue();

            if (commitTs <= tso && (removeTs == null || removeTs > tso)) {
                String suffix = fileName.substring(fileName.lastIndexOf('.') + 1);
                ColumnarFileType columnarFileType = ColumnarFileType.of(suffix);

                switch (columnarFileType) {
                case ORC:
                    snapshot.getOrcFiles().add(fileName);
                    break;
                case CSV:
                    snapshot.getCsvFiles().add(fileName);
                    break;
                case DEL:
                    snapshot.getDelFiles().add(fileName);
                    break;
                case SET:
                default:
                    // ignore.
                }
            }
        });

        totalCost = System.nanoTime() - startTime;

        if (SPLIT_MANAGER_LOGGER.isDebugEnabled()) {
            SPLIT_MANAGER_LOGGER.debug(MessageFormat.format("generateSnapshot for "
                    + "tableId = {0}, partName = {1}, "
                    + "tso = {2}, lastTso = {3}, "
                    + "totalCost = {4}, ioCost = {5}",
                tableId, partitionName, tso, latestTsoBackUp, totalCost, ioCost
            ));
        }

        return snapshot;
    }

    public synchronized void purge(long tso) {
        allPartsTsoInfo.forEach((partName, snapshot) -> {
            snapshot.entrySet().removeIf(entry -> {
                Long removeTs = entry.getValue().getValue();
                if (removeTs != null && removeTs <= tso) {
                    columnarManager.putPurgedFile(entry.getKey());
                    return true;
                } else {
                    return false;
                }
            });
        });
    }
}
