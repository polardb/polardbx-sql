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
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.oss.OSSFileType;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.schemaevolution.ColumnMetaWithTs;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarFileMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarFileMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.RoaringBitmap;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The instructions of Dynamic-columnar-manager:
 * 0. (Inner) Periodically fetch the newest tso from meta db.
 * 1. Find columnar-partition-snapshot of certain tso.
 * 2. Get file meta from file-meta-cache and columnar-snapshot-cache use findFileNames method.
 * 3. Use oss read option to generate orc-task / csv-task
 * 4. Use getDeleteBitMapOf method to generate delete bitmap for each task
 */
public class DynamicColumnarManager extends AbstractLifecycle implements ColumnarManager {
    private static final Logger LOGGER = LoggerFactory.getLogger("COLUMNAR_TRANS");

    private static final DynamicColumnarManager INSTANCE = new DynamicColumnarManager();

    public static final int MAXIMUM_FILE_META_COUNT = 1 << 18;
    public static final int MAXIMUM_SIZE_OF_SNAPSHOT_CACHE = 1 << 16;

    private FileVersionStorage versionStorage;
    private final Queue<String> filesToBePurged = new LinkedBlockingQueue<>();
    private final Object minTsoLock = new Object();
    private final Object latestTsoLock = new Object();
    /**
     * Cache all file-meta by its file name.
     */
    private LoadingCache<String, FileMeta> fileMetaCache;
    /**
     * Cache all Multi-version snapshot by schema name and table id.
     */
    private LoadingCache<Pair<String, Long>, MultiVersionColumnarSnapshot> snapshotCache;
    /**
     * columnar schema of each tso
     */
    private MultiVersionColumnarSchema columnarSchema;
    /**
     * Cache mapping: {partition-info, file-id} -> file-name
     */
    private LoadingCache<Pair<PartitionId, Integer>, Optional<String>> fileIdMapping;
    /**
     * cache append file last record: {tso, filename} -> {records}
     */
    private LoadingCache<Pair<Long, String>, List<ColumnarAppendedFilesRecord>> appendFileRecordCache;
    private final AtomicLong appendFileAccessCounter = new AtomicLong();
    private volatile Long minTso;
    private volatile Long latestTso;

    public DynamicColumnarManager() {
    }

    public static DynamicColumnarManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    public static List<ColumnarAppendedFilesRecord> getLatestAppendFileRecord(String fileName, Long tso) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarAppendedFilesAccessor columnarAppendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            columnarAppendedFilesAccessor.setConnection(connection);
            return columnarAppendedFilesAccessor.queryByFileNameAndMaxTso(fileName, tso);

        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, e,
                "fail to fetch append file record with file: " + fileName + " and tso: " + tso);
        }
    }

    public FileVersionStorage getVersionStorage() {
        return versionStorage;
    }

    /**
     * This method is for unit test ONLY
     */
    void injectForTest(FileVersionStorage versionStorage,
                       MultiVersionColumnarSchema multiVersionColumnarSchema) {
        this.versionStorage = versionStorage;
        this.versionStorage.open();
        this.columnarSchema = multiVersionColumnarSchema;
    }

    @Override
    protected void doInit() {
        this.versionStorage = new FileVersionStorage(this);
        this.versionStorage.open();
        this.columnarSchema = new MultiVersionColumnarSchema(this);

        // Build file meta cache
        this.fileMetaCache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_FILE_META_COUNT)
            .build(
                new CacheLoader<String, FileMeta>() {
                    @Override
                    public FileMeta load(@NotNull String fileName) {
                        List<FilesRecord> filesRecords = null;
                        try (Connection connection = MetaDbUtil.getConnection()) {
                            FilesAccessor filesAccessor = new FilesAccessor();
                            filesAccessor.setConnection(connection);

                            // query meta db && filter table files.
                            filesRecords = filesAccessor
                                .queryColumnarByFileName(fileName)
                                .stream()
                                .filter(filesRecord -> OSSFileType.of(filesRecord.fileType) == OSSFileType.TABLE_FILE)
                                .collect(Collectors.toList());
                        } catch (SQLException e) {
                            // ignore.
                        }

                        if (!filesRecords.isEmpty()) {
                            FileMeta fileMeta = FileMeta.parseFrom(filesRecords.get(0));

                            // fill with column meta.
                            List<ColumnMeta> columnMetas =
                                getColumnMetas(fileMeta.getSchemaTs(),
                                    Long.parseLong(fileMeta.getLogicalTableName()));
                            fileMeta.initColumnMetas(ColumnarStoreUtils.IMPLICIT_COLUMN_CNT, columnMetas);
                            return fileMeta;
                        }

                        return null;
                    }
                }
            );

        final DynamicColumnarManager self = this;
        // Build snapshot cache
        this.snapshotCache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_SIZE_OF_SNAPSHOT_CACHE)
            .build(new CacheLoader<Pair<String, Long>, MultiVersionColumnarSnapshot>() {
                @Override
                public MultiVersionColumnarSnapshot load(@NotNull Pair<String, Long> schemaAndTableId) {
                    return new MultiVersionColumnarSnapshot(
                        self, schemaAndTableId.getKey(), schemaAndTableId.getValue()
                    );
                }
            });

        // Build file-id mapping cache
        this.fileIdMapping = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_FILE_META_COUNT)
            .build(new CacheLoader<Pair<PartitionId, Integer>, Optional<String>>() {
                @Override
                public Optional<String> load(@NotNull Pair<PartitionId, Integer> key) throws Exception {
                    Integer columnarFileId = key.getValue();
                    PartitionId partitionId = key.getKey();
                    String logicalSchema = partitionId.getLogicalSchema();
                    String tableId = String.valueOf(partitionId.getTableId());
                    String partName = partitionId.getPartName();

                    List<ColumnarFileMappingRecord> records;
                    try (Connection connection = MetaDbUtil.getConnection()) {
                        ColumnarFileMappingAccessor accessor = new ColumnarFileMappingAccessor();
                        accessor.setConnection(connection);

                        records = accessor.queryByFileId(
                            logicalSchema, tableId, partName, columnarFileId
                        );
                    }

                    if (records != null && !records.isEmpty()) {
                        ColumnarFileMappingRecord record = records.get(0);
                        return Optional.of(record.getFileName());
                    }

                    return Optional.empty();
                }
            });

        this.appendFileRecordCache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_FILE_META_COUNT)
            .build(new CacheLoader<Pair<Long, String>, List<ColumnarAppendedFilesRecord>>() {
                @Override
                public List<ColumnarAppendedFilesRecord> load(@NotNull Pair<Long, String> tsoAndFileName) {
                    return getLatestAppendFileRecord(tsoAndFileName.getValue(), tsoAndFileName.getKey());
                }
            });

        LOGGER.info("Columnar Manager of has been initialized");
    }

    @Override
    public void purge(long tso) {
        // Update inner state of version chain
        if (minTso != null && minTso >= tso) {
            return;
        }

        synchronized (minTsoLock) {
            if (minTso != null && minTso >= tso) {
                return;
            }

            // update min tso before physical purge to make it safe
            minTso = tso;
            // this could collect all files which should be purged
            snapshotCache.asMap().values().forEach(snapshot -> snapshot.purge(tso));

            for (String fileName = nextPurgedFile(); fileName != null; fileName = nextPurgedFile()) {
                ColumnarFileType columnarFileType =
                    ColumnarFileType.of(fileName.substring(fileName.lastIndexOf('.') + 1));
                fileMetaCache.invalidate(fileName);

                if (columnarFileType.isDeltaFile()) {
                    versionStorage.purgeByFile(fileName);
                }
            }

            versionStorage.purge(tso);
            // TODO(siyun): purge when CCI is dropped

            // columnar fetches purge signal by SHOW COLUMNAR OFFSET
        }
    }

    public void putPurgedFile(String fileName) {
        filesToBePurged.add(fileName);
    }

    private String nextPurgedFile() {
        return filesToBePurged.poll();
    }

    @Override
    public Pair<List<FileMeta>, List<FileMeta>> findFiles(long tso, String logicalSchema, String logicalTable,
                                                          String partName) {
        if (tso == Long.MIN_VALUE) {
            return Pair.of(ImmutableList.of(), ImmutableList.of());
        }
        try {
            Long tableId = getTableId(tso, logicalSchema, logicalTable);
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot = snapshotCache.get(
                Pair.of(logicalSchema, tableId)
            ).generateSnapshot(partName, tso);

            List<FileMeta> csvFiles = new ArrayList<>();
            List<FileMeta> orcFiles = new ArrayList<>();

            // fetch orc file metas
            snapshot.getOrcFiles().stream().map(
                this::fileMetaOf
            ).forEach(orcFiles::add);

            // fetch csv file metas
            snapshot.getCsvFiles().stream().map(
                this::fileMetaOf
            ).forEach(csvFiles::add);

            return Pair.of(orcFiles, csvFiles);
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to generate columnar snapshot of tso: %d", tso));
        }
    }

    @Override
    public Pair<List<String>, List<String>> findFileNames(long tso, String logicalSchema, String logicalTable,
                                                          String partName) {
        if (tso == Long.MIN_VALUE) {
            return Pair.of(ImmutableList.of(), ImmutableList.of());
        }
        try {
            Long tableId = getTableId(tso, logicalSchema, logicalTable);
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot = snapshotCache.get(
                Pair.of(logicalSchema, tableId)
            ).generateSnapshot(partName, tso);
            return Pair.of(snapshot.getOrcFiles(), snapshot.getCsvFiles());
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to generate columnar snapshot of tso: %d", tso));
        }
    }

    public List<String> delFileNames(long tso, String logicalSchema, String logicalTable,
                                     String partName) {
        if (tso == Long.MIN_VALUE) {
            return ImmutableList.of();
        }

        try {
            MultiVersionColumnarSnapshot.ColumnarSnapshot snapshot = snapshotCache.get(
                Pair.of(logicalSchema, Long.valueOf(logicalTable))
            ).generateSnapshot(partName, tso);

            return snapshot.getDelFiles();
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT, e,
                String.format("Failed to generate columnar snapshot of tso: %d", tso));
        }
    }

    @Override
    public List<Chunk> csvData(long tso, String csvFileName) {
        appendFileAccessCounter.getAndIncrement();
        try {
            List<ColumnarAppendedFilesRecord> appendedFilesRecords =
                appendFileRecordCache.get(Pair.of(tso, csvFileName));
            if (appendedFilesRecords == null || appendedFilesRecords.isEmpty()) {
                return new ArrayList<>();
            } else {
                Preconditions.checkArgument(appendedFilesRecords.size() == 1);
                return versionStorage.csvData(appendedFilesRecords.get(0).checkpointTso, csvFileName);
            }
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_CSV_FILE, t,
                String.format("Failed to load csv file, filename: %s, tso: %d", csvFileName, tso));
        }
    }

    @Override
    public List<Chunk> rawCsvData(long tso, String csvFileName, ExecutionContext context) {
        appendFileAccessCounter.getAndIncrement();
        try {
            List<ColumnarAppendedFilesRecord> appendedFilesRecords =
                appendFileRecordCache.get(Pair.of(tso, csvFileName));
            if (appendedFilesRecords == null || appendedFilesRecords.isEmpty()) {
                return new ArrayList<>();
            } else {
                Preconditions.checkArgument(appendedFilesRecords.size() == 1);
                return versionStorage.csvRawOrcTypeData(appendedFilesRecords.get(0).checkpointTso,
                    csvFileName, context);
            }
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_LOAD_CSV_FILE, t,
                String.format("Failed to load csv file, filename: %s, tso: %d", csvFileName, tso));
        }
    }

    @Override
    public int fillSelection(String fileName, long tso, int[] selection, IntegerBlock positionBlock) {
        return versionStorage.fillSelection(fileName, tso, selection, positionBlock);
    }

    @Override
    public int fillSelection(String fileName, long tso, int[] selection, LongColumnVector longColumnVector,
                             int batchSize) {
        return versionStorage.fillSelection(fileName, tso, selection, longColumnVector, batchSize);
    }

    @Override
    public Optional<String> fileNameOf(String logicalSchema, long tableId, String partName, int columnarFileId) {
        PartitionId partitionId = PartitionId.of(partName, logicalSchema, tableId);
        try {
            return fileIdMapping.get(Pair.of(partitionId, columnarFileId));
        } catch (ExecutionException e) {
            fileIdMapping.invalidate(Pair.of(partitionId, columnarFileId));
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SCHEMA, e.getCause(),
                "Failed to fetch file id of partition: " + e.getCause().getMessage());
        }
    }

    @Override
    public FileMeta fileMetaOf(String fileName) {
        try {
            return fileMetaCache.get(fileName);
        } catch (ExecutionException e) {
            fileMetaCache.invalidate(fileName);
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SCHEMA, e.getCause(),
                "Failed to fetch file meta of file, file name: " + fileName);
        }
    }

    @Override
    public @NotNull List<Long> getColumnFieldIdList(long versionId, long tableId) {
        return columnarSchema.getColumnFieldIdList(versionId, tableId);
    }

    @Override
    public @NotNull List<ColumnMeta> getColumnMetas(long schemaTso, String logicalSchema, String logicalTable) {
        return getColumnMetas(schemaTso, getTableId(schemaTso, logicalSchema, logicalTable));
    }

    @Override
    public @NotNull List<ColumnMeta> getColumnMetas(long schemaTso, long tableId) {
        return columnarSchema.getColumnMetas(schemaTso, tableId);
    }

    @Override
    public @NotNull Map<Long, Integer> getColumnIndex(long schemaTso, long tableId) {
        return columnarSchema.getColumnIndexMap(schemaTso, tableId);
    }

    @Override
    public @NotNull ColumnMetaWithTs getInitColumnMeta(long tableId, long fieldId) {
        return columnarSchema.getInitColumnMeta(tableId, fieldId);
    }

    @Override
    public int[] getPrimaryKeyColumns(String fileName) {
        FileMeta fileMeta = fileMetaOf(fileName);
        long tableId = Long.parseLong(fileMeta.getLogicalTableName());
        long schemaTso = fileMeta.getSchemaTs();
        return columnarSchema.getPrimaryKeyColumns(schemaTso, tableId);
    }

    @Override
    public RoaringBitmap getDeleteBitMapOf(long tso, String fileName) {
        FileMeta fileMeta = fileMetaOf(fileName);
        return versionStorage.getDeleteBitMap(fileMeta, tso);
    }

    @Override
    public long latestTso() {
        if (latestTso != null) {
            return latestTso;
        }
        // Fetch the latest tso
        synchronized (latestTsoLock) {
            if (latestTso != null) {
                return latestTso;
            }

            Long gmsLatestTso = ColumnarTransactionUtils.getLatestTsoFromGms();
            latestTso = gmsLatestTso != null ? gmsLatestTso : Long.MIN_VALUE;
            return latestTso;
        }
    }

    @Override
    public void setLatestTso(long tso) {
        if (latestTso == null || latestTso < tso) {
            synchronized (latestTsoLock) {
                if (latestTso == null || latestTso < tso) {
                    latestTso = tso;
                }
            }
        }
    }

    private Long getTableId(long tso, String logicalSchema, String logicalTable) {
        try {
            return columnarSchema.getTableId(tso, logicalSchema, logicalTable);
        } catch (ExecutionException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SCHEMA, e.getCause(),
                String.format("Failed to fetch table id, tso: %d, schema name: %s, table name: %s",
                    tso, logicalSchema, logicalTable));
        }
    }

    public List<byte[][]> generatePacket() {
        return versionStorage.generatePacket();
    }

    @Override
    protected void doDestroy() {
        versionStorage.close();
    }

    @Override
    public List<Integer> getPhysicalColumnIndexes(long tso, String tableName, List<Integer> columnIndexes) {
        // TODO(siyun): NO MOCK
        return columnIndexes.stream().map(index -> index + ColumnarStoreUtils.IMPLICIT_COLUMN_CNT)
            .collect(Collectors.toList());
    }

    @Override
    public Map<Long, Integer> getPhysicalColumnIndexes(String fileName) {
        FileMeta fileMeta = fileMetaOf(fileName);
        long tableId = Long.parseLong(fileMeta.getLogicalTableName());
        long schemaTso = fileMeta.getSchemaTs();
        return columnarSchema.getColumnIndexMap(schemaTso, tableId);
    }

    @Override
    public List<Integer> getSortKeyColumns(long tso, String logicalSchema, String logicalTable) {
        try {
            return columnarSchema.getSortKeyColumns(tso, logicalSchema, logicalTable);
        } catch (ExecutionException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SCHEMA, e.getCause(),
                String.format("Failed to fetch sort key info, tso: %d, schema name: %s, table name: %s",
                    tso, logicalSchema, logicalTable));
        }
    }

    /**
     * 获取当前缓存的下水位线，每分钟更新一次，用于优化增量文件的加载和 purge
     *
     * @return 下水位线的一个下界
     */
    @NotNull
    public Long getMinTso() {
        if (minTso == null) {
            synchronized (minTsoLock) {
                if (minTso == null) {
                    minTso = ColumnarTransactionUtils.getMinColumnarSnapshotTime();
                }
            }
        }

        return minTso;
    }

    /**
     * approximate number of entries in this cache
     */
    public long getLoadedAppendFileCount() {
        return appendFileRecordCache.size();
    }

    /**
     * TODO
     */
    public long getLoadedVersionCount() {
        return 0;
    }

    public long getAppendFileAccessCount() {
        return appendFileAccessCounter.get();
    }

    @Override
    public void reload() {
        synchronized (minTsoLock) {
            minTso = null;
            synchronized (latestTsoLock) {
                latestTso = null;
                try {
                    snapshotCache.invalidateAll();
                    fileMetaCache.invalidateAll();
                    fileIdMapping.invalidateAll();
                    appendFileRecordCache.invalidateAll();
                    filesToBePurged.clear();
                } catch (Throwable t) {
                    // ignore
                }

                try {
                    this.columnarSchema = new MultiVersionColumnarSchema(this);
                } catch (Throwable t) {
                    // ignore
                }

                try {
                    this.versionStorage = new FileVersionStorage(this);
                    this.versionStorage.open();
                } catch (Throwable t) {
                    // ignore
                }
            }
        }

        LOGGER.info("Columnar Manager of has been reloaded");
    }

}
