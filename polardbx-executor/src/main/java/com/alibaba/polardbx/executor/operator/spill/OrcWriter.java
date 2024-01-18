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

package com.alibaba.polardbx.executor.operator.spill;

import com.alibaba.polardbx.common.CrcAccumulator;
import com.alibaba.polardbx.common.OrderInvariantHasher;
import com.alibaba.polardbx.common.oss.access.OSSKey;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.Configurable;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.repo.mysql.handler.LogicalLoadDataHandler;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.sql.OutFileParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.WriterImpl;
import org.jetbrains.annotations.NotNull;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.Engine.OSS;

public class OrcWriter {
    private final Logger
        logger = LoggerFactory.getLogger(OrcWriter.class);
    private Writer writer;
    private OrderInvariantHasher orderInvariantHasher;
    private @NotNull Configuration conf;
    private TypeDescription schema;
    private VectorizedRowBatch batch;
    private String orcRootPath = "../spill/temp/";
    private String filePath;
    private String fileName;
    int indexStride;
    private SessionProperties sessionProperties;
    private List<ColumnProvider> columnProviders;
    private List<DataType> dataTypes;
    private List<ColumnMeta> columnMetas;
    private long maxRowsPerFile;
    private long totalRows;
    private int fileId;
    private boolean ifUpload;
    private String ossPrefix = "oss://";
    private String ossPathFormat = "%s_%s%s";
    private List<OSSKey> ossKeys;
    private List<String> localFilePaths;

    public OrcWriter(ExecutionContext executionContext, OutFileParams outFileParams, DataType[] dataTypes) {
        this.maxRowsPerFile = executionContext.getParamManager().getLong(ConnectionParams.OSS_EXPORT_MAX_ROWS_PER_FILE);
        this.totalRows = 0;
        this.fileId = 1;

        ossKeys = new ArrayList<>();
        localFilePaths = new ArrayList<>();

        this.ifUpload = ossPrefix.equals(outFileParams.getFileName()
            .substring(0, Math.min(outFileParams.getFileName().length(), ossPrefix.length())));

        if (this.ifUpload) {
            this.fileName = outFileParams.getFileName().substring(outFileParams.getFileName().lastIndexOf('/') + 1);
            this.filePath = orcRootPath + fileName;
            localFilePaths.add(filePath);
            final String uniqueId = UUID.randomUUID().toString();
            OSSKey currentOssKey =
                OSSKey.createExportOrcFileOSSKey(fileName.substring(0, fileName.length() - 4), uniqueId);
            ossKeys.add(currentOssKey);
        } else {
            this.filePath = orcRootPath + outFileParams.getFileName();
        }

        this.conf = OrcMetaUtils.getConfiguration(executionContext);
        this.sessionProperties = SessionProperties.fromExecutionContext(executionContext);

        this.columnMetas = (List<ColumnMeta>) outFileParams.getColumnMeata();
        this.columnProviders =
            columnMetas.stream().map(ColumnProviders::getProvider)
                .collect(
                    Collectors.toList());
        this.dataTypes = Arrays.asList(dataTypes);

        List<Field> fieldList = new ArrayList<>();
        for (ColumnMeta columnMeta : columnMetas) {
            fieldList.add(
                new Field(columnMeta.getTableName(), columnMeta.getOriginColumnName(), columnMeta.getDataType()));
        }
        this.schema = OrcMetaUtils.getTypeDescription(fieldList);

        this.indexStride = (int) conf.getLong("orc.row.index.stride", 1000);
        batch = schema.createRowBatch(indexStride);

        this.orderInvariantHasher = new OrderInvariantHasher();
        Path path = new Path(filePath);
        try {
            path.getFileSystem(conf).setWriteChecksum(false);
            path.getFileSystem(conf).setVerifyChecksum(false);

            OrcFile.WriterOptions opts = OrcFile.writerOptions(conf).setSchema(schema);
            writer = new WriterImpl(path.getFileSystem(opts.getConfiguration()), path, opts);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void writeRows(List<Row> rows) {
        for (Row row : rows) {
            totalRows++;
            int rowNumber = batch.size++;
            CrcAccumulator accumulator = new CrcAccumulator();
            for (int columnId = 0; columnId < dataTypes.size(); columnId++) {
                ColumnProvider columnProvider = columnProviders.get(columnId);
                ColumnVector columnVector = batch.cols[columnId];
                DataType dataType = dataTypes.get(columnId);

                // data convert
                columnProvider
                    .putRow(columnVector, rowNumber, row, columnId, dataType,
                        sessionProperties.getTimezone(), Optional.ofNullable(accumulator));
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
                if (totalRows >= maxRowsPerFile) {
                    initNextFile();
                }
            }
        }
    }

    private void initNextFile() {
        // example : example_1.orc example_2.orc ...
        String currentLocalFilePath =
            String.format(ossPathFormat, filePath.substring(0, filePath.length() - 4), fileId, ".orc");

        if (ifUpload) {
            localFilePaths.add(currentLocalFilePath);
            final String uniqueId = UUID.randomUUID().toString();
            String currentFileName = fileName.substring(0, fileName.length() - 4) + "_" + fileId;
            OSSKey currentOssKey =
                OSSKey.createExportOrcFileOSSKey(currentFileName, uniqueId);
            ossKeys.add(currentOssKey);
        }

        fileId++;

        File tmpFile = new File(currentLocalFilePath);
        if (tmpFile.exists()) {
            tmpFile.delete();
        }

        this.orderInvariantHasher = new OrderInvariantHasher();
        Path path = new Path(currentLocalFilePath);
        try {
            writer.close();
            path.getFileSystem(conf).setWriteChecksum(false);
            path.getFileSystem(conf).setVerifyChecksum(false);

            OrcFile.WriterOptions opts = OrcFile.writerOptions(conf).setSchema(schema);
            writer = new WriterImpl(path.getFileSystem(opts.getConfiguration()), path, opts);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
        totalRows = 0;
    }

    public void writeRowsFinish() {
        try {
            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
            writer.close();
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void uploadToOss() {
        if (!ifUpload) {
            return;
        }
        try {
            for (int i = 0; i < localFilePaths.size(); i++) {
                File localFile = new File(localFilePaths.get(i));
                FileSystemUtils.writeFile(localFile, ossKeys.get(i).toString(), OSS);
                logger.info("file upload done: " + localFilePaths.get(i) + " file size = " + localFile.length());
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void close(boolean needClearFile) {
        if (needClearFile) {
            File tmpFile = new File(filePath);
            if (tmpFile.exists()) {
                tmpFile.delete();
            }
        }
    }
}
