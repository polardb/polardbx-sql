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

import com.alibaba.polardbx.common.CrcAccumulator;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.OrderInvariantHasher;
import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.orc.OrcBloomFilter;
import com.alibaba.polardbx.common.oss.OSSMetaLifeCycle;
import com.alibaba.polardbx.common.oss.access.OSSKey;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.meta.FileStorageBackFillAccessor;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetaAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.PolarDBXOrcSchema;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcConf;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.gsi.utils.Transformer.buildColumnParam;

public class OSSBackFillWriterTask {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

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

    private TableMeta sourceTableMeta;

    private TableMeta targetTableMeta;
    private Engine engine;

    /* ======= column providers to put value ======= */
    private List<ColumnProvider> columnProviders;
    private List<ColumnProvider> bfColumnProviders;

    private long totalRows;
    final private long maxRowsPerFile;
    final private boolean removeTmpFiles;

    private volatile int currentFileIndex;
    private List<OSSKey> ossKeys;
    private List<String> localFilePaths;
    private List<Long> tableRows;

    private List<Long> fileChecksum;
    private OrderInvariantHasher orderInvariantHasher;

    /**
     * blocking queue controls the of flushing data to oss.
     * To support pause and continue ddl, the queue must be FIFO,
     * and the flush process must be single-thread
     */
    private BlockingQueue<FutureTask> flushTaskBlockingQueue;
    private List<Future> flushTaskList;

    private List<Row> lowerRows;
    private List<Row> upperRows;

    /**
     * TODO(shengyu): use this to support checker when restart
     */
    private boolean restart;
    private List<ParameterContext> lastPK;

    private Long taskId;
    /**
     * the primary key of file in system table FILES
     */
    private List<Long> filePrimaryKeys;

    Optional<OSSBackFillTimer> timeoutCheck;

    public OSSBackFillWriterTask(String logicalSchema,
                                 String logicalTable,
                                 String physicalSchema,
                                 String physicalTable,
                                 String sourcePhySchema,
                                 String sourcePhyTable,
                                 TableMeta sourceTableMeta,
                                 TableMeta targetTableMeta,
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

        this.sourceTableMeta = sourceTableMeta;
        this.targetTableMeta = targetTableMeta;
        this.engine = engine;

        this.taskName = physicalSchema + "/" + physicalTable;

        this.currentFileIndex = 0;
        this.localFilePaths = new ArrayList<>();
        this.ossKeys = new ArrayList<>();
        this.tableRows = new ArrayList<>();

        this.flushTaskBlockingQueue = new ArrayBlockingQueue<>(1);
        this.flushTaskList = new ArrayList<>();

        int indexStride = (int) conf.getLong("orc.row.index.stride", 1000);
        this.batch = schema.createRowBatch(getRowBatchVersion(conf), indexStride);

        this.fpp = conf.getDouble("orc.bloom.filter.fpp", 0.01D);

        this.totalRows = 0L;
        this.maxRowsPerFile = maxRowsPerFile;
        this.removeTmpFiles = removeTmpFiles;

        this.lowerRows = new ArrayList<>();
        this.upperRows = new ArrayList<>();

        this.filePrimaryKeys = new ArrayList<>();
        this.restart = false;
        this.lastPK = null;
        this.timeoutCheck = Optional.empty();
        this.fileChecksum = new ArrayList<>();
    }

    private TypeDescription.RowBatchVersion getRowBatchVersion(Configuration conf) {
        boolean enableDecimal64 = OrcConf.ENABLE_DECIMAL_64.getBoolean(conf);
        return enableDecimal64 ? TypeDescription.RowBatchVersion.USE_DECIMAL64 :
            TypeDescription.RowBatchVersion.ORIGINAL;
    }

    public OSSBackFillWriterTask(String logicalSchema,
                                 String logicalTable,
                                 String physicalSchema,
                                 String physicalTable,
                                 String sourcePhySchema,
                                 String sourcePhyTable,
                                 TableMeta sourceTableMeta,
                                 Engine engine,
                                 Long taskId,
                                 Configuration conf,
                                 String physicalPartitionName,
                                 PolarDBXOrcSchema orcSchema,
                                 TableMeta targetTableMeta,
                                 long maxRowsPerFile,
                                 boolean removeTmpFiles,
                                 ExecutionContext ec,
                                 boolean enablePause) {
        this(logicalSchema, logicalTable, physicalSchema, physicalTable, sourcePhySchema, sourcePhyTable,
            sourceTableMeta, targetTableMeta,
            engine, taskId, conf, physicalPartitionName, orcSchema, maxRowsPerFile, removeTmpFiles);
        if (enablePause) {
            this.timeoutCheck = Optional.of(new OSSBackFillTimer(sourcePhySchema, sourcePhyTable, ec));
        }
    }

    /**
     * check whether enabling broken-point continuing
     *
     * @return true if enabling pause and continue ddl
     */
    private boolean enablePause() {
        return timeoutCheck.isPresent();
    }

    public void consume(Cursor cursor, List<Map<Integer, ParameterContext>> mockResult, ExecutionContext ec) {
        timeoutCheck.ifPresent(a -> a.checkTime(ec));

        if (ec.getDdlContext().isInterrupted()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + ec.getDdlContext().getJobId() + "' has been interrupted");
        }
        Row row = cursor.next();
        if (row == null) {
            return;
        }

        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(ec);

        // check if we need to prepare the next file / writer
        initNextFile();

        Row latestRow = null;

        // for checksum
        CrcAccumulator accumulator = new CrcAccumulator();
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
                        .putRow(columnVector, rowNumber, row, columnId - 1, dataType,
                            sessionProperties.getTimezone(), Optional.ofNullable(accumulator));
                } else {
                    // data convert with redundant sort key
                    ColumnVector redundantColumnVector = batch.cols[redundantColumnId - 1];
                    columnProvider.putRow(columnVector, redundantColumnVector, rowNumber, row, columnId - 1, dataType,
                        sessionProperties.getTimezone(), Optional.ofNullable(accumulator));
                }
            }

            // Merge the crc result of the last row.
            long crcResult = accumulator.getResult();
            orderInvariantHasher.add(crcResult);
            accumulator.reset();

            // flush the batch to disk
            if (batch.size == batch.getMaxSize()) {
                try {
                    writer.addRowBatch(batch);
                    batch.reset();
                } catch (IOException e) {
                    throw GeneralUtil.nestedException(e);
                }
            }

            latestRow = new ArrayRow(row.getParentCursorMeta(), row.getValues().toArray());
            row = cursor.next();
            // mock the params
            mockResult.add(ImmutableMap.of());
        }
        // record the upper bound
        upperRows.set(upperRows.size() - 1, latestRow);

        // trans the last row to params
        if (latestRow != null) {
            mockResult.set(mockResult.size() - 1, buildParams(latestRow));
        }

        LOGGER.info(
            "task = " + this.getTaskName() + ", current file name = " + this.localFilePaths.get(this.currentFileIndex)
                + ", current row = " + this.getTotalRows());

        if (totalRows >= maxRowsPerFile) {
            flush(ec);
        }
    }

    public synchronized void flush(ExecutionContext ec) {
        if (ec.getDdlContext().isInterrupted()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + ec.getDdlContext().getJobId() + "' has been interrupted");
        }
        if (this.currentFileIndex == this.localFilePaths.size()) {
            // nothing to flush
            return;
        }

        // push up the current file index
        final int fileIndex = this.currentFileIndex++;

        // update file checksum
        fileChecksum.add(orderInvariantHasher.getResult());

        // finish local write, written to tmp
        finishLocalWrite();

        FailPoint.injectRandomException();

        // build flush task
        FutureTask flushTask = new FutureTask<>(() -> {
            try {
                return doFlush(ec, new MetaForCommit(fileIndex));
            } finally {
                // Poll in finally to prevent deadlock on putting blockingQueue.
                flushTaskBlockingQueue.poll();
            }
        });
        flushTaskList.add(flushTask);

        try {
            // maybe the latest flush task is not finished.
            flushTaskBlockingQueue.put(flushTask);
        } catch (InterruptedException e) {
            // clear all tasks
            cancelAsync();
            flushTaskList.clear();
            throw GeneralUtil.nestedException(e);
        }

        // submit async flush task
        ServiceProvider.getInstance().getServerExecutor()
            .submit(this.logicalTable, this.taskId.toString(), AsyncTask.build(flushTask));

        totalRows = 0L;
    }

    private Map<Integer, ParameterContext> buildParams(Row latestRow) {
        final List<ColumnMeta> columns = latestRow.getParentCursorMeta().getColumns();
        final Map<Integer, ParameterContext> params = new HashMap<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            final ParameterContext parameterContext = buildColumnParam(latestRow, i);
            params.put(i + 1, parameterContext);
        }
        return params;
    }

    /**
     * check whether the orc file is right or not
     */
    private boolean checkTable(MetaForCommit metaForCommit, ExecutionContext ec) {
        OSSBackFillChecker checker = new OSSBackFillChecker(
            sourceTableMeta.getSchemaName(), sourceTableMeta.getTableName(),
            loadTablePhysicalSchemaName, loadTablePhysicalTableName,
            metaForCommit.getLocalFilePath(),
            conf, noRedundantSchema, redundantId, dataTypes, columnProviders, physicalPartitionName,
            metaForCommit.getLowerRow(), metaForCommit.getUpperRow()
        );
        return checker.checkTable(ec);
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

    public long doFlush(ExecutionContext ec, MetaForCommit metaForCommit) {
        if (ec.getDdlContext().isInterrupted()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + ec.getDdlContext().getJobId() + "' has been interrupted");
        }
        // check oss file
        if (targetTableMeta != null && !Engine.isFileStore(targetTableMeta.getEngine()) && ec.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_FILE_STORE_CHECK_TABLE)
            && !ec.getParamManager().getBoolean(ConnectionParams.ENABLE_EXPIRE_FILE_STORAGE_PAUSE)) {
            // only check non-file-store source table.
            GsiUtils.wrapWithSingleDbTrx(ExecutorContext.getContext(ec.getSchemaName()).getTransactionManager(),
                ec, (selectEc) -> checkTable(metaForCommit, selectEc));
        }

        long start = System.currentTimeMillis();

        // do upload task

        FailPoint.injectRandomException();
        // upload local file to OSS instance.
        upload(metaForCommit);

        // make local index for oss table
        if (shouldPutBloomFilter()) {
            putBloomFilter(metaForCommit);
        }

        // commit the file logically
        commitFile(metaForCommit);

        // delete file from local.
        if (removeTmpFiles) {
            File tmpFile = new File(metaForCommit.getLocalFilePath());
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        }

        return System.currentTimeMillis() - start;
    }

    /**
     * 'commit' the file to oss.
     * Once the method if called, the file is ready and shouldn't be rollback when restarting the ddl
     *
     * @param metaForCommit all meta need for committing
     */
    private void commitFile(MetaForCommit metaForCommit) {
        ByteBuffer tailBuffer = this.getSerializedTail(metaForCommit.getLocalFilePath());
        byte[] fileMeta = new byte[tailBuffer.remaining()];
        tailBuffer.get(fileMeta);

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            try {
                MetaDbUtil.beginTransaction(metaDbConn);
                // first validate all the meta of oss
                FilesAccessor filesAccessor = new FilesAccessor();
                filesAccessor.setConnection(metaDbConn);

                if (shouldPutBloomFilter()) {
                    ColumnMetaAccessor columnMetaAccessor = new ColumnMetaAccessor();
                    columnMetaAccessor.setConnection(metaDbConn);

                    // validate files of bloom filter
                    filesAccessor.validByFileName(
                        columnMetaAccessor.queryByTableFileName(metaForCommit.getOssKey().toString())
                            .stream().map(x -> x.bloomFilterPath).collect(Collectors.toList()));
                    // validate column metas of the file
                    columnMetaAccessor.validByTableFileName(metaForCommit.getOssKey().toString());
                }

                long rowCount = metaForCommit.getTableRow();
                // validate the file
                filesAccessor.validFile(metaForCommit.getFilePrimaryKey(), fileMeta, this.getFileSize(), rowCount,
                    metaForCommit.getOrcHash());

                // support breakpoint continue
                if (enablePause()) {
                    FileStorageBackFillAccessor fileStorageBackFillAccessor =
                        new FileStorageBackFillAccessor(this.taskId,
                            this.loadTablePhysicalSchemaName, this.loadTablePhysicalTableName);
                    fileStorageBackFillAccessor.setConnection(metaDbConn);
                    Map<Integer, ParameterContext> lastRow = buildParams(metaForCommit.getUpperRow());
                    // record position mark uploaded to oss
                    Preconditions.checkArgument(
                        fileStorageBackFillAccessor.selectCountBackfillObjectFromFileStorage() > 0);
                    // update existing record
                    List<GsiBackfillManager.BackfillObjectRecord> records =
                        fileStorageBackFillAccessor.selectBackfillObjectFromFileStorage();
                    for (GsiBackfillManager.BackfillObjectRecord record : records) {
                        record.setLastValue(Transformer.serializeParam(lastRow.get((int) record.getColumnIndex() + 1)));
                        record.setSuccessRowCount(record.getSuccessRowCount() + rowCount);
                        record.setEndTime(
                            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()));
                    }
                    fileStorageBackFillAccessor.updateBackfillObjectToFileStorage(records);
                }
                MetaDbUtil.commit(metaDbConn);
                // used for debug
                timeoutCheck.ifPresent(OSSBackFillTimer::setFlushed);

            } catch (Exception e) {
                MetaDbUtil.rollback(metaDbConn, e, null, null);
                LOGGER.error(e.getMessage());
                throw GeneralUtil.nestedException(e);
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
            }
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
            this.tableRows.add(totalRows);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public ByteBuffer getSerializedTail(String localFilePath) {
        try {
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
        filesRecord.localPartitionName = physicalPartitionName;

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            return TableMetaChanger
                .addOssFileAndReturnLastInsertId(metaDbConn, logicalSchema, logicalTable, filesRecord);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private boolean shouldPutBloomFilter() {
        return this.bfSchema != null && !this.bfSchema.getChildren().isEmpty();
    }

    public void putBloomFilter(MetaForCommit metaForCommit) {
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
            String localFilePath = metaForCommit.getLocalFilePath();

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

                        storeColumnMeta(metaForCommit.getOssKey(), metaKey,
                            lastOffset, stripeIndex, stripe, colId, colName, writtenBytes);
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
            filesRecord.localPartitionName = physicalPartitionName;

            primaryKey = TableMetaChanger
                .addOssFileAndReturnLastInsertId(metaDbConn, logicalSchema, logicalTable, filesRecord);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }

        // upload to oss
        FileSystemUtils.writeFile(localIndexFile, metaKey.toString(), this.engine, false);

        // change file size
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            TableMetaChanger.changeOssFile(metaDbConn, primaryKey, localIndexFile.length());
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private void storeColumnMeta(OSSKey fileKey, OSSKey metaKey, int lastOffset, int stripeIndex,
                                 StripeInformation stripe, int colId, String colName, int writtenBytes) {
        // store the column meta for <file, stripe, column>
        ColumnMetasRecord columnMetasRecord = new ColumnMetasRecord();
        columnMetasRecord.tableFileName = fileKey.toString();
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

    public void upload(MetaForCommit metaForCommit) {
        try {
            String localFilePath = metaForCommit.getLocalFilePath();
            OSSKey ossKey = metaForCommit.getOssKey();

            File localFile = new File(localFilePath);
            this.fileSize = localFile.length();
            LOGGER.info("orc generation done: " + localFilePath);
            LOGGER.info("file size(in bytes): " + fileSize);

            FileSystemUtils.writeFile(localFile, ossKey.toString(), this.engine, false);
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
                lowerRows.add(null);
            } else {
                lowerRows.add(upperRows.get(upperRows.size() - 1));
            }
            upperRows.add(null);

            int[] mapList = new int[columnProviders.size()];
            for (int i = 0; i < mapList.length; i++) {
                mapList[i] = i;
            }
            orderInvariantHasher = new OrderInvariantHasher();
            Path path = new Path(currentLocalFilePath);
            try {
                path.getFileSystem(conf).setWriteChecksum(false);
                path.getFileSystem(conf).setVerifyChecksum(false);
                OrcFile.WriterOptions opts = OrcFile.writerOptions(conf).setSchema(schema);
                this.writer = new WriterImpl(path.getFileSystem(opts.getConfiguration()), path, opts);
            } catch (IOException e) {
                throw GeneralUtil.nestedException(e);
            }

            this.totalRows = 0L;
        }

    }

    public void setRestart() {
        restart = true;
        // todo(sheng) : prepare primary key for lower bound

    }

    /**
     * meta needed when flushing the file to oss
     */
    class MetaForCommit {
        OSSKey ossKey;
        String localFilePath;
        Long tableRow;
        Long orcHash;
        Row lowerRow;
        Row upperRow;
        // TODO(shengyu): make checker support restarting ddl
        List<ParameterContext> lastPk;
        Long filePrimaryKey;

        public MetaForCommit(int index) {
            this.ossKey = ossKeys.get(index);
            this.localFilePath = localFilePaths.get(index);
            this.tableRow = tableRows.get(index);
            this.orcHash = fileChecksum.get(index);
            this.lowerRow = lowerRows.get(index);
            this.upperRow = upperRows.get(index);
            if (restart && index == 0) {
                this.lastPk = lastPK;
            }
            this.filePrimaryKey = filePrimaryKeys.get(index);
        }

        public OSSKey getOssKey() {
            return ossKey;
        }

        public String getLocalFilePath() {
            return localFilePath;
        }

        public Long getTableRow() {
            return tableRow;
        }

        public Long getOrcHash() {
            return orcHash;
        }

        public Row getLowerRow() {
            return lowerRow;
        }

        public Row getUpperRow() {
            return upperRow;
        }

        public Long getFilePrimaryKey() {
            return filePrimaryKey;
        }

        public List<ParameterContext> getLastPk() {
            return lastPk;
        }
    }
}
