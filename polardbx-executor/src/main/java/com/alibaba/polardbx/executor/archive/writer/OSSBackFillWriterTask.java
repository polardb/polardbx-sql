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

package com.alibaba.polardbx.executor.archive.writer;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.orc.OrcBloomFilter;
import com.alibaba.polardbx.common.oss.OSSMetaLifeCycle;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.common.oss.access.OSSKey;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.executor.workqueue.PriorityWorkQueue;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.PolarDBXOrcSchema;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.WriterImpl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.gsi.utils.Transformer.buildColumnParam;
import static com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils.ORC_BLOOM_FILTER_FPP;
import static com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils.ORC_ROW_INDEX_STRIDE;

public class OSSBackFillWriterTask {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    private static final long MAX_FILE_SIZE = 512 * 1024 * 1024;

    private String taskName;
    private VectorizedRowBatch batch;
    private Writer writer;
    private long fileSize;

    private Configuration conf;
    private double fpp;

    private String physicalPartitionName;

    String physicalSchema;
    String physicalTable;

    String logicalSchema;
    String logicalTable;

    /* ======= PolarDB-X orc schema ======= */
    private PolarDBXOrcSchema polarDBXOrcSchema;
    private TypeDescription schema;
    private List<ColumnMeta> columnMetas;
    private TypeDescription bfSchema;
    private List<ColumnMeta> bfColumnMetas;
    private int redundantId;
    private int[] redundantMap;
    private List<ColumnMeta> redundantColumnMetas;
    private boolean[] noRedundantSchema;

    private List<DataType> dataTypes;

    private String loadTablePhysicalSchemaName;
    private String loadTablePhysicalTableName;

    private TableMeta tableMeta;
    private Engine engine;

    /* ======= column providers to put value ======= */
    private List<ColumnProvider> columnProviders;
    private List<ColumnProvider> bfColumnProviders;

    private long totalRows;
    private long currentBytes;
    final private long maxRowsPerFile;
    final private boolean removeTmpFiles;

    private volatile int currentFileIndex;
    private List<OSSKey> ossKeys;
    private List<String> localFilePaths;
    private List<Long> tableRowsList;

    private BlockingQueue<FutureTask> flushTaskBlockingQueue;
    private List<Future> flushTaskList;

    private List<Row> lowerRow;
    private List<Row> upperRow;

    private Long taskId;
    private List<Long> filePrimaryKeys;

    public OSSBackFillWriterTask(String logicalSchema,
                                 String logicalTable,
                                 String physicalSchema,
                                 String physicalTable,
                                 String sourcePhySchema,
                                 String sourcePhyTable,
                                 TableMeta tableMeta,
                                 Engine engine,
                                 Long taskId,
                                 Configuration conf,
                                 String physicalPartitionName,
                                 PolarDBXOrcSchema orcSchema,
                                 long maxRowsPerFile, boolean removeTmpFiles) {
        this.logicalTable = logicalTable;
        this.logicalSchema = logicalSchema;
        this.physicalSchema = physicalSchema;
        this.physicalTable = physicalTable;
        this.loadTablePhysicalSchemaName = sourcePhySchema;
        this.loadTablePhysicalTableName = sourcePhyTable;
        this.taskId = taskId;
        this.conf = conf;
        this.physicalPartitionName = physicalPartitionName;

        // NOTE: orc schema = {column schema + redundant schema}
        this.polarDBXOrcSchema = orcSchema;
        this.schema = orcSchema.getSchema();
        this.bfSchema = orcSchema.getBfSchema();
        this.columnMetas = orcSchema.getColumnMetas();
        this.bfColumnMetas = orcSchema.getBfColumnMetas();
        this.redundantId = orcSchema.getRedundantId();
        this.redundantMap = orcSchema.getRedundantMap();
        this.redundantColumnMetas = orcSchema.getRedundantColumnMetas();
        this.noRedundantSchema = orcSchema.buildNoRedundantSchema();

        this.dataTypes = columnMetas.stream().map(ColumnMeta::getDataType).collect(Collectors.toList());
        // column providers
        this.columnProviders = ColumnProviders.getColumnProviders(this.polarDBXOrcSchema);
        this.bfColumnProviders = ColumnProviders.getBfColumnProviders(this.polarDBXOrcSchema);

        this.tableMeta = tableMeta;
        this.engine = engine;

        this.taskName = physicalSchema + "/" + physicalTable;

        this.currentFileIndex = 0;
        this.localFilePaths = new ArrayList<>();
        this.ossKeys = new ArrayList<>();
        this.tableRowsList = new ArrayList<>();

        this.flushTaskBlockingQueue = new ArrayBlockingQueue<>(1);
        this.flushTaskList = new ArrayList<>();

        int indexStride = (int) conf.getLong(ORC_ROW_INDEX_STRIDE, 1000);
        this.batch = schema.createRowBatch(indexStride);

        this.fpp = conf.getDouble(ORC_BLOOM_FILTER_FPP, 0.01D);

        this.totalRows = 0L;
        this.currentBytes = 0L;
        this.maxRowsPerFile = maxRowsPerFile;
        this.removeTmpFiles = removeTmpFiles;
        this.lowerRow = new ArrayList<>();
        this.upperRow = new ArrayList<>();
        this.filePrimaryKeys = new ArrayList<>();
    }

    public void consume(Cursor cursor, List<Map<Integer, ParameterContext>> mockResult, ExecutionContext ec) {
        Row row = cursor.next();
        if (row == null) {
            return;
        }

        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(ec);

        // check if we need to prepare the next file / writer
        initNextFile();

        Row latestRow = null;

        while (row != null) {
            // main loop.
            totalRows++;
            int rowNumber = batch.size++;
            for (int columnId = 1; columnId < redundantId; columnId++) {
                ColumnProvider columnProvider = columnProviders.get(columnId - 1);
                ColumnVector columnVector = batch.cols[columnId - 1];
                DataType dataType = dataTypes.get(columnId - 1);

                int redundantColumnId = redundantMap[columnId - 1];
                if (redundantColumnId == -1) {
                    // data convert
                    columnProvider
                        .putRow(columnVector, rowNumber, row, columnId - 1, dataType, sessionProperties.getTimezone());
                } else {
                    // data convert with redundant sort key
                    ColumnVector redundantColumnVector = batch.cols[redundantColumnId - 1];
                    columnProvider.putRow(columnVector, redundantColumnVector, rowNumber, row, columnId - 1, dataType,
                        sessionProperties.getTimezone());
                }
            }

            // flush the batch to disk
            if (batch.size == batch.getMaxSize()) {
                try {
                    writer.addRowBatch(batch);
                    batch.reset();

                    updateCurrentBytes();
                } catch (IOException e) {
                    throw GeneralUtil.nestedException(e);
                }
            }

            latestRow = row;
            row = cursor.next();
            // mock the params
            mockResult.add(ImmutableMap.of());
        }
        // record the upper bound
        upperRow.set(upperRow.size() - 1, latestRow);

        // trans the last row to params
        if (latestRow != null) {
            final List<ColumnMeta> columns = latestRow.getParentCursorMeta().getColumns();

            final Map<Integer, ParameterContext> params = new HashMap<>(columns.size());
            for (int i = 0; i < columns.size(); i++) {

                final ParameterContext parameterContext = buildColumnParam(latestRow, i);

                params.put(i + 1, parameterContext);
            }
            mockResult.set(mockResult.size() - 1, params);
        }

        LOGGER.info(
            "task = " + this.getTaskName() + ", current file name = " + this.localFilePaths.get(this.currentFileIndex)
                + ", current row = " + this.getTotalRows());

        if (totalRows >= maxRowsPerFile
            || currentBytes >= MAX_FILE_SIZE) {
            flush(ec);
        }
    }

    private void updateCurrentBytes() {
        String localFilePath = this.localFilePaths.get(currentFileIndex);
        File file = new File(localFilePath);
        if (file.exists()) {
            this.currentBytes = file.length();
        }
    }

    public synchronized void flush(ExecutionContext ec) {
        if (this.currentFileIndex == this.localFilePaths.size()) {
            // nothing to flush
            return;
        }

        // push up the current file index
        final int fileIndex = this.currentFileIndex++;

        // finish local write, written to tmp, thus there is no need to
        finishLocalWrite();

        FailPoint.injectRandomException();

        // build flush task
        FutureTask flushTask = new FutureTask<>(() -> {
            try {
                return doFlush(ec, fileIndex);
            } finally {
                // Poll in finally to prevent dead lock on putting blockingQueue.
                flushTaskBlockingQueue.poll();
            }
        });
        flushTaskList.add(flushTask);

        try {
            // maybe the latest flush task is not finished.
            flushTaskBlockingQueue.put(flushTask);
        } catch (InterruptedException e) {
            // clear all tasks
            AsyncUtils.waitAll(flushTaskList);
            throw GeneralUtil.nestedException(e);
        }

        // submit async flush task
        PriorityWorkQueue.getInstance()
            .executeWithContext(flushTask, PriorityFIFOTask.TaskPriority.OSS_FLUSH);

        totalRows = 0L;
    }

    /**
     * check whether the orc file is right or not
     */
    private boolean checkTable(int fileIndex, ExecutionContext ec) {
        Row lowRow = this.lowerRow.get(fileIndex);
        Row upperRow = this.upperRow.get(fileIndex);
        OSSBackFillChecker checker = new OSSBackFillChecker(
            tableMeta.getSchemaName(), tableMeta.getTableName(),
            loadTablePhysicalSchemaName, loadTablePhysicalTableName,
            localFilePaths.get(fileIndex),
            conf, noRedundantSchema, redundantId, dataTypes, columnProviders, physicalPartitionName,
            lowRow, upperRow
        );
        return checker.checkTable(fileIndex, ec);
    }

    public synchronized void waitAsync() {
        AsyncUtils.waitAll(flushTaskList);
    }

    public synchronized void cancelAsync() {
        flushTaskList.forEach(f -> {
            try {
                f.cancel(true);
            } catch (Throwable ignore) {
            }
        });
    }

    public long doFlush(ExecutionContext ec, int fileIndex) {
        // check oss file
        if (!Engine.isFileStore(tableMeta.getEngine()) && ec.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_FILE_STORE_CHECK_TABLE)) {
            // only check non-file-store source table.
            GsiUtils.wrapWithSingleDbTrx(ExecutorContext.getContext(ec.getSchemaName()).getTransactionManager(),
                ec, (selectEc) -> checkTable(fileIndex, selectEc));
        }

        long start = System.currentTimeMillis();

        // do upload task
        String localFilePath = this.localFilePaths.get(fileIndex);

        FailPoint.injectRandomException();

        // upload local file to OSS instance.
        upload(fileIndex);

        // update file meta blob / file size / row count.
        updateFileMeta(fileIndex);

        // make local index for oss table
        if (this.bfSchema != null && !this.bfSchema.getChildren().isEmpty()) {
            putBloomFilter(fileIndex);
        }

        // delete file from local.
        if (removeTmpFiles) {
            File tmpFile = new File(localFilePath);
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        }

        return System.currentTimeMillis() - start;
    }

    private void updateFileMeta(int fileIndex) {
        ByteBuffer tailBuffer = this.getSerializedTail(fileIndex);
        byte[] fileMeta = new byte[tailBuffer.remaining()];
        tailBuffer.get(fileMeta);

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            TableMetaChanger.changeOssFile(metaDbConn, filePrimaryKeys.get(fileIndex), fileMeta, this.getFileSize(),
                fileIndex >= this.tableRowsList.size() ? 0L : this.tableRowsList.get(fileIndex));
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void finishLocalWrite() {
        try {
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
            writer.close();
            this.tableRowsList.add(totalRows);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public ByteBuffer getSerializedTail(int fileIndex) {
        try {
            String localFilePath = this.localFilePaths.get(fileIndex);
            Configuration conf = new Configuration();
            Reader reader = OrcFile.createReader(new Path(localFilePath),
                OrcFile.readerOptions(conf));
            return reader.getSerializedFileFooter();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Long putFileMeta(int fileIndex) {
        // construct files record
        FilesRecord filesRecord = new FilesRecord();
        filesRecord.fileName = this.getOssKey(fileIndex).toString();
        filesRecord.fileType = this.getOssKey(fileIndex).getFileType().toString();
        filesRecord.fileMeta = new byte[] {};
        filesRecord.tableCatalog = "";
        filesRecord.tableSchema = this.getPhysicalSchema();
        filesRecord.tableName = this.getPhysicalTable();
        filesRecord.engine = this.engine.name();
        filesRecord.taskId = taskId;
        filesRecord.status = "";
        filesRecord.lifeCycle = OSSMetaLifeCycle.CREATING.ordinal();
        filesRecord.localPath = this.localFilePaths.get(fileIndex);
        filesRecord.logicalSchemaName = logicalSchema;
        filesRecord.logicalTableName = logicalTable;

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            return TableMetaChanger
                .addOssFileAndReturnLastInsertId(metaDbConn, logicalSchema, logicalTable, filesRecord);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void putBloomFilter(int fileIndex) {
        try {
            // prepare for index file
            final String uniqueId = UUID.randomUUID().toString();
            OSSKey metaKey = OSSKey.createBloomFilterFileOSSKey(
                this.getPhysicalSchema(),
                this.getPhysicalTable(),
                uniqueId,
                "",
                0
            );
            String localIndexFilePath = metaKey.localPath();
            File localIndexFile = new File(localIndexFilePath);

            int lastOffset, currentOffset = 0;

            // construct for all index key.
            // for each orc file
            String localFilePath = this.getLocalFilePath(fileIndex);
            try (FileOutputStream outputStream = new FileOutputStream(localIndexFile);
                Reader reader = OrcFile.createReader(new Path(localFilePath), OrcFile.readerOptions(conf))) {

                List<StripeInformation> stripes = reader.getStripes();
                for (int stripeIndex = 0; stripeIndex < stripes.size(); stripeIndex++) {
                    // for each stripe
                    StripeInformation stripe = stripes.get(stripeIndex);
                    Reader.Options readerOptions = new Reader.Options(conf)
                        .schema(bfSchema)
                        .range(stripe.getOffset(), stripe.getLength());

                    long stripeRows = stripe.getNumberOfRows();
                    final List<TypeDescription> children = bfSchema.getChildren();

                    // <column id> - bloom filter
                    Map<Integer, OrcBloomFilter> bloomFilterMap = new HashMap<>();
                    for (TypeDescription child : children) {
                        OrcBloomFilter bloomFilter = new OrcBloomFilter(stripeRows, fpp);
                        bloomFilterMap.put(child.getId(), bloomFilter);
                    }

                    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(readerOptions)) {
                        VectorizedRowBatch batch = bfSchema.createRowBatch();
                        while (rows.nextBatch(batch)) {
                            int batchSize = batch.size;
                            for (int col = 0; col < children.size(); col++) {
                                // for each column, put vector to bloom filter.
                                TypeDescription child = children.get(col);
                                ColumnVector vector = batch.cols[col];
                                OrcBloomFilter bf = bloomFilterMap.get(child.getId());
                                bfColumnProviders.get(col).putBloomFilter(vector, bf, 0, batchSize);
                            }
                        }
                    }

                    for (int col = 0; col < children.size(); col++) {
                        // for each column in this stripe,
                        // upload meta file, and record meta info.
                        int colId = children.get(col).getId();
                        String colName = bfSchema.getFieldNames().get(col);

                        // serialize the bloom-filter data to local file
                        // update files table
                        OrcBloomFilter bf = bloomFilterMap.get(colId);
                        int writtenBytes = OrcBloomFilter.serialize(outputStream, bf);
                        lastOffset = currentOffset;
                        currentOffset += writtenBytes;

                        storeColumnMeta(fileIndex, metaKey, lastOffset, stripeIndex, stripe, colId, colName,
                            writtenBytes);
                    }
                }

                storeIndexFileMeta(metaKey, localIndexFilePath, localIndexFile);
            } finally {
                if (removeTmpFiles) {
                    if (localIndexFile.exists()) {
                        localIndexFile.delete();
                    }
                }
            }
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private void storeIndexFileMeta(OSSKey metaKey, String localIndexFilePath, File localIndexFile) throws IOException {
        // handle index file
        // write to metaDB files table.
        Long primaryKey = null;
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            FilesRecord filesRecord = new FilesRecord();
            filesRecord.fileName = metaKey.toString();
            filesRecord.fileType = metaKey.getFileType().toString();
            filesRecord.tableSchema = this.getPhysicalSchema();
            filesRecord.tableName = this.getPhysicalTable();
            filesRecord.tableCatalog = "";
            filesRecord.engine = this.engine.name();
            filesRecord.taskId = taskId;
            filesRecord.lifeCycle = OSSMetaLifeCycle.CREATING.ordinal();
            filesRecord.localPath = localIndexFilePath;
            filesRecord.status = "";
            filesRecord.logicalSchemaName = logicalSchema;
            filesRecord.logicalTableName = logicalTable;

            primaryKey = TableMetaChanger
                .addOssFileAndReturnLastInsertId(metaDbConn, logicalSchema, logicalTable, filesRecord);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }

        // upload to oss
        FileSystemUtils.writeFile(localIndexFile, metaKey.toString(), this.engine);

        // change file size
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            TableMetaChanger.changeOssFile(metaDbConn, primaryKey, localIndexFile.length());
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private void storeColumnMeta(int fileIndex, OSSKey metaKey, int lastOffset, int stripeIndex,
                                 StripeInformation stripe, int colId, String colName, int writtenBytes) {
        // store the column meta for <file, stripe, column>
        ColumnMetasRecord columnMetasRecord = new ColumnMetasRecord();
        columnMetasRecord.tableFileName = this.getOssKey(fileIndex).toString();
        columnMetasRecord.tableName = this.getPhysicalTable();
        columnMetasRecord.tableSchema = this.getPhysicalSchema();
        columnMetasRecord.stripeIndex = stripeIndex;
        columnMetasRecord.stripeOffset = stripe.getOffset();
        columnMetasRecord.stripeLength = stripe.getLength();
        columnMetasRecord.columnName = colName;
        columnMetasRecord.columnIndex = colId;
        columnMetasRecord.bloomFilterPath = metaKey.toString();
        columnMetasRecord.bloomFilterOffset = lastOffset;
        columnMetasRecord.bloomFilterLength = writtenBytes;
        columnMetasRecord.isMerged = 1;
        columnMetasRecord.taskId = taskId;
        columnMetasRecord.lifeCycle = OSSMetaLifeCycle.CREATING.ordinal();
        columnMetasRecord.engine = this.engine.name();
        columnMetasRecord.logicalSchemaName = logicalSchema;
        columnMetasRecord.logicalTableName = logicalTable;

        // write column meta before writing bloom filter to oss
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            TableMetaChanger
                .addOssColumnMeta(metaDbConn, logicalSchema, logicalTable, columnMetasRecord);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void upload(int fileIndex) {
        try {
            String localFilePath = this.localFilePaths.get(fileIndex);
            OSSKey ossKey = this.ossKeys.get(fileIndex);

            File localFile = new File(localFilePath);
            this.fileSize = localFile.length();
            LOGGER.info("orc generation done: " + localFilePath);
            LOGGER.info("file size(in bytes): " + fileSize);

            FileSystemUtils.writeFile(localFile, ossKey.toString(), this.engine);
            LOGGER.info("file upload done: " + taskName);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public OSSKey getOssKey(int fileIndex) {
        return this.ossKeys.get(fileIndex);
    }

    public String getTaskName() {
        return taskName;
    }

    public String getLocalFilePath(int fileIndex) {
        return this.localFilePaths.get(fileIndex);
    }

    public TypeDescription getSchema() {
        return schema;
    }

    public VectorizedRowBatch getBatch() {
        return batch;
    }

    public Writer getWriter() {
        return writer;
    }

    public String getPhysicalSchema() {
        return physicalSchema;
    }

    public String getPhysicalTable() {
        return physicalTable;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public String getPhysicalPartitionName() {
        return physicalPartitionName;
    }

    public void setPhysicalPartitionName(String physicalPartitionName) {
        this.physicalPartitionName = physicalPartitionName;
    }

    private void initNextFile() {
        // need new file / writer for current file index
        if (this.currentFileIndex == this.localFilePaths.size()) {

            final String uniqueId = UUID.randomUUID().toString();

            String currentLocalFilePath =
                OSSKey.localFilePath(physicalSchema, physicalTable, uniqueId);
            this.localFilePaths.add(currentLocalFilePath);
            OSSKey currentOssKey =
                TStringUtil.isEmpty(physicalPartitionName)
                    ? OSSKey.createTableFileOSSKey(physicalSchema, physicalTable, uniqueId)
                    : OSSKey.createTableFileOSSKey(physicalSchema, physicalTable, physicalPartitionName, uniqueId);
            this.ossKeys.add(currentOssKey);
            File tmpFile = new File(currentLocalFilePath);
            if (tmpFile.exists()) {
                tmpFile.delete();
            }

            // update file metas to meta db
            this.filePrimaryKeys.add(putFileMeta(localFilePaths.size() - 1));

            if (this.localFilePaths.size() == 1) {
                lowerRow.add(null);
            } else {
                lowerRow.add(upperRow.get(upperRow.size() - 1));
            }
            upperRow.add(null);

            Path path = new Path(currentLocalFilePath);
            OrcFile.WriterOptions opts = OrcFile.writerOptions(conf).setSchema(schema);
            try {
                this.writer = new WriterImpl(path.getFileSystem(opts.getConfiguration()), path, opts);
            } catch (IOException e) {
                throw GeneralUtil.nestedException(e);
            }

            this.totalRows = 0L;
        }
    }
}
