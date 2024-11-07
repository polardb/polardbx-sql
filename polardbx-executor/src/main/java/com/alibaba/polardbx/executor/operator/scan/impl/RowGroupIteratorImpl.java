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

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.operator.scan.BlockCacheManager;
import com.alibaba.polardbx.executor.operator.scan.CacheReader;
import com.alibaba.polardbx.executor.operator.scan.ColumnReader;
import com.alibaba.polardbx.executor.operator.scan.LogicalRowGroup;
import com.alibaba.polardbx.executor.operator.scan.RowGroupIterator;
import com.alibaba.polardbx.executor.operator.scan.StripeLoader;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BigBitType;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.TypeUtils;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class RowGroupIteratorImpl implements RowGroupIterator<Block, ColumnStatistics> {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");
    /**
     * Stripe id of this row-group iterator.
     */
    protected final int stripeId;
    /**
     * The effective row-group count starting from the given startRowGroupId.
     */
    private final int effectiveGroupCount;
    /**
     * The starting row-group id from which the iterator enumerate the logical row-groups.
     */
    private final int startRowGroupId;
    /**
     * The bitmap of selected row-groups, shared by all scan-works in one split.
     * The length of bitmap rowGroupIncluded is equal to count of row-groups in stripe.
     */
    private final boolean[] rowGroupIncluded;
    /**
     * The column ids of primary keys in the file.
     * It may be null.
     */
    private final int[] primaryKeyColIds;
    // parameters for IO processing
    private final ExecutorService ioExecutor;
    private final FileSystem fileSystem;
    private final Configuration configuration;
    protected final Path filePath;
    // for compression
    private final int compressionSize;
    private final CompressionKind compressionKind;
    // preheated meta of this stripe
    private final PreheatFileMeta preheatFileMeta;
    // context for stripe parser
    private final StripeInformation stripeInformation;
    private final long startRowOfStripe;
    protected final TypeDescription fileSchema;
    private final OrcFile.WriterVersion version;
    private final ReaderEncryption encryption;
    protected final OrcProto.ColumnEncoding[] encodings;
    private final boolean ignoreNonUtf8BloomFilter;
    private final long maxBufferSize;
    private final int indexStride;
    protected final boolean[] columnIncluded;
    protected final int chunkLimit;
    protected final BlockCacheManager<Block> blockCacheManager;
    protected final OSSColumnTransformer ossColumnTransformer;
    protected final ExecutionContext context;
    private final boolean enableMetrics;
    private final boolean enableDecimal64;
    private final int maxDiskRangeChunkLimit;
    private final long maxMergeDistance;
    private final boolean enableBlockCache;
    private final MemoryAllocatorCtx memoryAllocatorCtx;
    /**
     * Metrics in scan-work level.
     */
    protected RuntimeMetrics metrics;
    private OrcIndex orcIndex;
    private StripeLoader stripeLoader;
    /**
     * The mapping from columnId to column-reader.
     */
    protected Map<Integer, ColumnReader> columnReaders;
    /**
     * The Mapping from columnId to cache-reader.
     */
    protected Map<Integer, CacheReader<Block>> cacheReaders;
    /**
     * The current effective group id.
     */
    protected int currentGroupId;
    private int nextGroupId;
    /**
     * The fetched row group count must be less than or equal to effectiveGroupCount.
     */
    private int fetchedRowGroupCount;
    /**
     * To store the fetched row-groups with it's groupId.
     */
    protected Map<Integer, LogicalRowGroup<Block, ColumnStatistics>> rowGroupMap;

    public RowGroupIteratorImpl(
        RuntimeMetrics metrics,

        // selected stripe and row-groups
        int stripeId, int startRowGroupId, int effectiveGroupCount, boolean[] rowGroupIncluded,
        // for primary key
        int[] primaryKeyColIds,
        // to execute the io task
        ExecutorService ioExecutor,
        FileSystem fileSystem, Configuration configuration, Path filePath,
        // for compression
        int compressionSize, CompressionKind compressionKind,
        // preheated meta of this stripe
        PreheatFileMeta preheatFileMeta,
        // context for stripe parser
        StripeInformation stripeInformation, long startRowOfStripe,
        TypeDescription fileSchema, OrcFile.WriterVersion version,
        ReaderEncryption encryption, OrcProto.ColumnEncoding[] encodings, boolean ignoreNonUtf8BloomFilter,
        long maxBufferSize, int maxDiskRangeChunkLimit, long maxMergeDistance, int chunkLimit,
        BlockCacheManager<Block> blockCacheManager,
        OSSColumnTransformer ossColumnTransformer,
        ExecutionContext context, boolean[] columnIncluded, int indexStride,
        // chunk size config
        // global block cache manager
        boolean enableDecimal64, MemoryAllocatorCtx memoryAllocatorCtx) {
        this.metrics = metrics;
        this.stripeId = stripeId;
        this.startRowGroupId = startRowGroupId;
        this.effectiveGroupCount = effectiveGroupCount;
        this.rowGroupIncluded = rowGroupIncluded;
        this.primaryKeyColIds = primaryKeyColIds;
        this.ioExecutor = ioExecutor;
        this.fileSystem = fileSystem;
        this.configuration = configuration;
        this.filePath = filePath;
        this.compressionSize = compressionSize;
        this.compressionKind = compressionKind;
        this.preheatFileMeta = preheatFileMeta;
        this.stripeInformation = stripeInformation;
        this.startRowOfStripe = startRowOfStripe;
        this.fileSchema = fileSchema;
        this.version = version;
        this.encryption = encryption;
        this.encodings = encodings;
        this.ignoreNonUtf8BloomFilter = ignoreNonUtf8BloomFilter;
        this.maxBufferSize = maxBufferSize;
        this.maxDiskRangeChunkLimit = maxDiskRangeChunkLimit;
        this.maxMergeDistance = maxMergeDistance;
        this.indexStride = indexStride;
        this.columnIncluded = columnIncluded;
        this.chunkLimit = chunkLimit;
        this.blockCacheManager = blockCacheManager;
        this.ossColumnTransformer = ossColumnTransformer;
        this.context = context;
        this.enableMetrics = context.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_COLUMNAR_METRICS);
        this.enableDecimal64 = enableDecimal64;
        this.enableBlockCache = context.getParamManager().getBoolean(ConnectionParams.ENABLE_BLOCK_CACHE);
        this.memoryAllocatorCtx = memoryAllocatorCtx;
        init();
    }

    static ColumnReader getDecimalReader(boolean enableDecimal64, ExecutionContext context, DataType inputType,
                                         int colId, boolean isPrimaryKey,
                                         StripeLoader stripeLoader, OrcIndex orcIndex,
                                         RuntimeMetrics metrics, int indexStride,
                                         OrcProto.ColumnEncoding encoding, boolean enableMetrics) {

        if (enableDecimal64 && TypeUtils.isDecimal64Precision(inputType.getPrecision())) {
            // whether to read a decimal64-encoded column into a decimal64 block
            boolean readIntoDecimal64 = context.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_COLUMNAR_DECIMAL64);
            if (readIntoDecimal64) {
                return new LongColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                    encoding.getKind(),
                    indexStride, enableMetrics);
            } else {
                return new Decimal64ToDecimalColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                    encoding.getKind(),
                    indexStride, enableMetrics);
            }
        } else {
            switch (encoding.getKind()) {
            case DIRECT:
            case DIRECT_V2: {
                return new DecimalColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                    enableMetrics);
            }
            case DICTIONARY:
            case DICTIONARY_V2:
                return new DictionaryDecimalColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                    encoding, indexStride, enableMetrics);
            default:
                throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
            }
        }
    }

    private void init() {
        this.stripeLoader = new AsyncStripeLoader(
            ioExecutor, fileSystem, configuration, filePath,
            columnIncluded, compressionSize, compressionKind,
            preheatFileMeta,
            stripeInformation, fileSchema, version, encryption,
            encodings, ignoreNonUtf8BloomFilter, maxBufferSize,
            maxDiskRangeChunkLimit, maxMergeDistance, metrics,
            enableMetrics, memoryAllocatorCtx);
        stripeLoader.open();

        this.orcIndex = preheatFileMeta.getOrcIndex(
            stripeInformation.getStripeId()
        );

        columnReaders = new HashMap<>();
        cacheReaders = new HashMap<>();

        // Build cache readers for each column
        for (int colId = 1; colId <= fileSchema.getMaximumId(); colId++) {
            if (columnIncluded[colId]) {
                CacheReader<Block> cacheReader = new CacheReaderImpl(stripeId, colId, rowGroupIncluded.length);
                cacheReaders.put(colId, cacheReader);
            }
        }

        boolean enableMetrics = context.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_METRICS);

        // the col id is precious identifier in orc file schema, while the col index is just the index in list.
        for (int colIndex = 0; colIndex < ossColumnTransformer.columnCount(); colIndex++) {
            Integer colId = ossColumnTransformer.getLocInOrc(colIndex);

            // if this column is missing in ORC file, skip
            // which means this column should be filled with default value
            if (colId == null) {
                continue;
            }

            // Build column readers according to type-description and encoding kind.
            // check if this col id belong to primary keys.
            boolean isPrimaryKey = false;
            if (primaryKeyColIds != null) {
                for (int primaryKeyId : primaryKeyColIds) {
                    if (primaryKeyId == colId) {
                        isPrimaryKey = true;
                        break;
                    }
                }
            }
            OrcProto.ColumnEncoding encoding = encodings[colId];
            Preconditions.checkNotNull(encoding);

            // NOTE: we need more type-specified implementations
            TypeDescription typeDescription = fileSchema.getChildren().get(colId - 1);
            DataType inputType = ossColumnTransformer.getSourceColumnMeta(colIndex).getDataType();
            Preconditions.checkNotNull(
                inputType);                // Generate specific column reader for each column according to data type.
            if (context.isEnableOrcRawTypeBlock()) {
                // Special path for check cci consistency.
                // Normal oss read should not get here.
                // Only read the following data types: long, double, float, byte[]
                generateOrcRawTypeColumnReader(enableMetrics, colId, encoding, typeDescription, inputType,
                    isPrimaryKey);
            } else {
                generateColumnReader(enableMetrics, colId, encoding, typeDescription, inputType, isPrimaryKey);
            }
        }

        currentGroupId = -1;
        nextGroupId = -1;
        rowGroupMap = new HashMap<>();
    }

    private void generateColumnReader(boolean enableMetrics, int colId, OrcProto.ColumnEncoding encoding,
                                      TypeDescription typeDescription, DataType inputType, boolean isPrimaryKey) {
        switch (inputType.fieldType()) {
        case MYSQL_TYPE_LONGLONG: {
            if (!inputType.isUnsigned()) {
                // for signed bigint.
                ColumnReader columnReader =
                    new LongColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding.getKind(),
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
            } else {
                // for unsigned bigint
                ColumnReader columnReader =
                    new UnsignedLongColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding.getKind(),
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
            }
            break;
        }
        case MYSQL_TYPE_SHORT: {
            if (!inputType.isUnsigned()) {
                // for signed short
                ColumnReader columnReader =
                    new ShortColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding.getKind(),
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
            } else {
                // for unsigned short
                ColumnReader columnReader =
                    new IntegerColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding.getKind(),
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
            }
            break;
        }
        case MYSQL_TYPE_INT24: {
            // for signed/unsigned unsigned mediumint
            ColumnReader columnReader =
                new IntegerColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                    encoding.getKind(),
                    indexStride, enableMetrics);
            columnReaders.put(colId, columnReader);
            break;
        }
        case MYSQL_TYPE_TINY: {
            if (!inputType.isUnsigned()) {
                // for signed tiny
                ColumnReader columnReader =
                    new ByteColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding.getKind(),
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
            } else {
                // for unsigned tiny
                ColumnReader columnReader =
                    new ShortColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding.getKind(),
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
            }
            break;
        }
        case MYSQL_TYPE_LONG: {
            if (!inputType.isUnsigned()) {
                // for signed int
                ColumnReader columnReader =
                    new IntegerColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding.getKind(),
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
            } else {
                // for unsigned int
                ColumnReader columnReader =
                    new LongColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding.getKind(),
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
            }
            break;
        }
        case MYSQL_TYPE_FLOAT: {
            ColumnReader columnReader =
                new FloatColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, indexStride,
                    enableMetrics);
            columnReaders.put(colId, columnReader);
            break;
        }
        case MYSQL_TYPE_DOUBLE: {
            ColumnReader columnReader =
                new DoubleColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, indexStride,
                    enableMetrics);
            columnReaders.put(colId, columnReader);
            break;
        }
        case MYSQL_TYPE_YEAR:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2: {
            ColumnReader columnReader =
                new LongColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, encoding.getKind(),
                    indexStride,
                    enableMetrics);
            columnReaders.put(colId, columnReader);
            break;
        }
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2: {
            ColumnReader columnReader =
                new TimestampColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                    encoding.getKind(),
                    indexStride,
                    enableMetrics, context);
            columnReaders.put(colId, columnReader);
            break;
        }
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2: {
            // for date
            ColumnReader columnReader =
                new PackedTimeColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                    encoding.getKind(),
                    indexStride, enableMetrics);
            columnReaders.put(colId, columnReader);
            break;
        }
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL: {
            ColumnReader columnReader = getDecimalReader(enableDecimal64, context, inputType, colId, isPrimaryKey,
                stripeLoader, orcIndex, metrics, indexStride, encoding, enableMetrics);
            columnReaders.put(colId, columnReader);
            break;
        }
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VARCHAR: {
            if (inputType instanceof BinaryType) {
                switch (encoding.getKind()) {
                case DIRECT:
                case DIRECT_V2: {
                    // For column in direct encoding
                    ColumnReader columnReader =
                        new DirectBinaryColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                            indexStride,
                            enableMetrics);
                    columnReaders.put(colId, columnReader);
                    break;
                }
                case DICTIONARY:
                case DICTIONARY_V2:
                    // for dictionary encoding
                    ColumnReader columnReader =
                        new DictionaryBinaryColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                            encoding,
                            indexStride, enableMetrics);
                    columnReaders.put(colId, columnReader);
                    break;
                default:
                    throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
                }
            } else if (inputType instanceof SliceType) {
                boolean enableSliceDict =
                    context.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_SLICE_DICT);
                switch (encoding.getKind()) {
                case DIRECT:
                case DIRECT_V2: {
                    // For column in direct encoding
                    ColumnReader columnReader =
                        new DirectVarcharColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                            indexStride,
                            enableMetrics);
                    columnReaders.put(colId, columnReader);
                    break;
                }
                case DICTIONARY:
                case DICTIONARY_V2:
                    // for dictionary encoding
                    ColumnReader columnReader =
                        new DictionaryVarcharColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                            encoding,
                            indexStride, enableMetrics, enableSliceDict);
                    columnReaders.put(colId, columnReader);
                    break;
                default:
                    throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
                }
                break;
            } else {
                throw new UnsupportedOperationException(
                    "String type not supported: " + inputType.getClass().getName());
            }
            break;
        }
        case MYSQL_TYPE_ENUM: {
            switch (encoding.getKind()) {
            case DIRECT:
            case DIRECT_V2: {
                // For column in direct encoding
                ColumnReader columnReader =
                    new DirectEnumColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        indexStride,
                        enableMetrics, inputType);
                columnReaders.put(colId, columnReader);
                break;
            }
            case DICTIONARY:
            case DICTIONARY_V2:
                // for dictionary encoding
                ColumnReader columnReader =
                    new DictionaryEnumColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding,
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
                break;
            default:
                throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
            }

            break;
        }
        case MYSQL_TYPE_JSON: {
            switch (encoding.getKind()) {
            case DIRECT:
            case DIRECT_V2: {
                // For column in direct encoding
                ColumnReader columnReader =
                    new DirectJsonColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, indexStride,
                        enableMetrics, inputType);
                columnReaders.put(colId, columnReader);
                break;
            }
            case DICTIONARY:
            case DICTIONARY_V2:
                // for dictionary encoding
                ColumnReader columnReader =
                    new DictionaryJsonColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding, indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
                break;
            default:
                throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
            }
            break;
        }
        case MYSQL_TYPE_BIT: {
            if (inputType instanceof BigBitType) {
                ColumnReader columnReader =
                    new BigBitColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding.getKind(),
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
            } else {
                // for bit
                ColumnReader columnReader =
                    new IntegerColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding.getKind(),
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
            }
            break;
        }
        case MYSQL_TYPE_BLOB: {
            switch (encoding.getKind()) {
            case DIRECT:
            case DIRECT_V2: {
                // For column in direct encoding
                ColumnReader columnReader =
                    new DirectBlobColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        indexStride,
                        enableMetrics);
                columnReaders.put(colId, columnReader);
                break;
            }
            case DICTIONARY:
            case DICTIONARY_V2:
                // for dictionary encoding
                ColumnReader columnReader =
                    new DictionaryBlobColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics,
                        encoding,
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
                break;
            default:
                throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
            }
            break;
        }

        default:
            throw GeneralUtil.nestedException("Unsupported type-description " + typeDescription);
        }
    }

    private void generateOrcRawTypeColumnReader(boolean enableMetrics, int colId, OrcProto.ColumnEncoding encoding,
                                                TypeDescription typeDescription, DataType inputType,
                                                boolean isPrimaryKey) {
        switch (inputType.fieldType()) {
        case MYSQL_TYPE_LONGLONG:
        case MYSQL_TYPE_SHORT:
        case MYSQL_TYPE_INT24:
        case MYSQL_TYPE_TINY:
        case MYSQL_TYPE_LONG:
        case MYSQL_TYPE_YEAR:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2:
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2:
        case MYSQL_TYPE_BIT: {
            ColumnReader columnReader =
                new LongColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, encoding.getKind(),
                    indexStride, enableMetrics);
            columnReaders.put(colId, columnReader);
            break;
        }
        case MYSQL_TYPE_FLOAT: {
            ColumnReader columnReader =
                new DoubleBlockFloatColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, indexStride,
                    enableMetrics);
            columnReaders.put(colId, columnReader);
            break;
        }
        case MYSQL_TYPE_DOUBLE: {
            ColumnReader columnReader =
                new DoubleColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, indexStride,
                    enableMetrics);
            columnReaders.put(colId, columnReader);
            break;
        }
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL: {
            if (enableDecimal64 && TypeUtils.isDecimal64Precision(inputType.getPrecision())) {
                // Long reader.
                ColumnReader columnReader =
                    new LongColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, encoding.getKind(),
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
            } else {
                // Byte array reader.
                ColumnReader columnReader =
                    new DirectBinaryColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, indexStride,
                        enableMetrics);
                columnReaders.put(colId, columnReader);
            }
            break;
        }
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VARCHAR: {
            if (inputType instanceof BinaryType || inputType instanceof SliceType) {
                switch (encoding.getKind()) {
                case DIRECT:
                case DIRECT_V2: {
                    // For column in direct encoding
                    ColumnReader columnReader =
                        new DirectBinaryColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, indexStride,
                            enableMetrics);
                    columnReaders.put(colId, columnReader);
                    break;
                }
                case DICTIONARY:
                case DICTIONARY_V2:
                    // for dictionary encoding
                    ColumnReader columnReader =
                        new DictionaryBinaryColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, encoding,
                            indexStride, enableMetrics);
                    columnReaders.put(colId, columnReader);
                    break;
                default:
                    throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
                }
            } else {
                throw new UnsupportedOperationException(
                    "String type not supported: " + inputType.getClass().getName());
            }
            break;
        }
        case MYSQL_TYPE_ENUM:
        case MYSQL_TYPE_BLOB: {
            switch (encoding.getKind()) {
            case DIRECT:
            case DIRECT_V2: {
                // For column in direct encoding
                ColumnReader columnReader =
                    new DirectBinaryColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, indexStride,
                        enableMetrics);
                columnReaders.put(colId, columnReader);
                break;
            }
            case DICTIONARY:
            case DICTIONARY_V2:
                // for dictionary encoding
                ColumnReader columnReader =
                    new DictionaryBinaryColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, encoding,
                        indexStride, enableMetrics);
                columnReaders.put(colId, columnReader);
                break;
            default:
                throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
            }

            break;
        }
        case MYSQL_TYPE_JSON: {
            ColumnReader columnReader =
                new DirectBinaryColumnReader(colId, isPrimaryKey, stripeLoader, orcIndex, metrics, indexStride,
                    enableMetrics);
            columnReaders.put(colId, columnReader);
            break;
        }

        default:
            throw GeneralUtil.nestedException("Unsupported type-description " + typeDescription);
        }
    }

    protected int getRowCount(int groupId) {
        final long rowsInStripe = stripeInformation.getNumberOfRows();
        int groupsInStripe = (int) ((rowsInStripe + indexStride - 1) / indexStride);

        if (groupId != groupsInStripe - 1) {
            return indexStride;
        } else {
            return (int) (rowsInStripe - (groupsInStripe - 1) * indexStride);
        }
    }

    protected int startRowId(int groupId) {
        return (int) (groupId * indexStride + startRowOfStripe);
    }

    @Override
    public void seek(int rowId) {
        currentGroupId = rowId / indexStride;
    }

    @Override
    public LogicalRowGroup<Block, ColumnStatistics> current() {
        LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup = rowGroupMap.computeIfAbsent(
            currentGroupId, any -> new LogicalRowGroupImpl(
                metrics,
                filePath, stripeId, currentGroupId,
                getRowCount(currentGroupId), startRowId(currentGroupId),
                fileSchema, ossColumnTransformer, encodings, columnIncluded, chunkLimit,
                columnReaders, cacheReaders, blockCacheManager,
                context)
        );
        return logicalRowGroup;
    }

    @Override
    public BlockCacheManager<Block> getCacheManager() {
        return blockCacheManager;
    }

    @Override
    public boolean hasNext() {
        // start from param: startRowGroupId
        // end with bitmap length or fetchedRowGroupCount
        for (int groupId = currentGroupId + 1;
             groupId < rowGroupIncluded.length && fetchedRowGroupCount < effectiveGroupCount;
             groupId++) {
            if (groupId >= startRowGroupId && rowGroupIncluded[groupId]) {
                nextGroupId = groupId;
                fetchedRowGroupCount++;
                return true;
            }
        }
        nextGroupId = -1;
        return false;
    }

    @Override
    public Void next() {
        // check if the next group exist.
        Preconditions.checkArgument(nextGroupId != -1);
        currentGroupId = nextGroupId;
        return null;
    }

    @Override
    public StripeLoader getStripeLoader() {
        return this.stripeLoader;
    }

    @Override
    public ColumnReader getColumnReader(int columnId) {
        return this.columnReaders.get(columnId);
    }

    @Override
    public CacheReader<Block> getCacheReader(int columnId) {
        CacheReader<Block> cacheReader = this.cacheReaders.get(columnId);
        if (!enableBlockCache && !cacheReader.isInitialized()) {
            // Initialize cache reader with empty map to forbid the block cache.
            cacheReader.initialize(new HashMap<>());
        }
        return cacheReader;
    }

    @Override
    public Path filePath() {
        return this.filePath;
    }

    @Override
    public int stripeId() {
        return this.stripeId;
    }

    @Override
    public void noMoreChunks() {
        columnReaders.forEach((colId, colReader) -> {
            colReader.setNoMoreBlocks();
        });
    }

    @Override
    public boolean[] columnIncluded() {
        return this.columnIncluded;
    }

    @Override
    public boolean[] rgIncluded() {
        return rowGroupIncluded;
    }

    @Override
    public void close(boolean force) {
        columnReaders.forEach((colId, colReader) -> {
            if (LOGGER.isDebugEnabled()) {
                // In force mode, or ref count of reader is zero.
                LOGGER.debug(MessageFormat.format(
                    "when row group iterator closing, colId = {0}, refCount = {1}, noMoreBlock = {2}, workId = {3}",
                    colId, colReader.refCount(), colReader.hasNoMoreBlocks(), metrics.name()
                ));
            }
            if (force || (colReader.refCount() <= 0 && colReader.hasNoMoreBlocks())) {
                colReader.close();
            }
        });
    }

    public boolean checkIfAllReadersClosed() {
        AtomicBoolean allClosed = new AtomicBoolean(true);
        columnReaders.forEach((colId, colReader) -> {
            if (!colReader.isClosed()) {
                allClosed.set(false);
            }
        });
        return allClosed.get();
    }
}
