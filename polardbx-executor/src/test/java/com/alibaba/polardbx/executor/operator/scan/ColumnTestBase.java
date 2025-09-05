package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.BigIntegerBlock;
import com.alibaba.polardbx.executor.chunk.BlobBlock;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.ByteArrayBlock;
import com.alibaba.polardbx.executor.chunk.ByteBlock;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DoubleBlock;
import com.alibaba.polardbx.executor.chunk.EnumBlock;
import com.alibaba.polardbx.executor.chunk.FloatBlock;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ShortBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.executor.chunk.TimeBlock;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
import com.alibaba.polardbx.executor.chunk.ULongBlock;
import com.alibaba.polardbx.executor.operator.scan.impl.AsyncStripeLoader;
import com.alibaba.polardbx.executor.operator.scan.impl.BigBitColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.ByteColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DecimalColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryBinaryColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryBlobColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryEnumColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryJsonColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryVarcharColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DirectBinaryColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DirectBlobColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DirectEnumColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DirectVarcharColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DoubleColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.FloatColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.IntegerColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.DirectJsonColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.LongColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.PackedTimeColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.PreheatFileMeta;
import com.alibaba.polardbx.executor.operator.scan.impl.ShortColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.StaticStripePlanner;
import com.alibaba.polardbx.executor.operator.scan.impl.TimestampColumnReader;
import com.alibaba.polardbx.executor.operator.scan.impl.UnsignedLongColumnReader;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileAccumulatorType;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileUnit;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BigBitType;
import com.alibaba.polardbx.optimizer.core.datatype.BinaryType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.StreamName;
import org.apache.orc.impl.TypeUtils;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public abstract class ColumnTestBase {
    public static final int DEFAULT_CHUNK_LIMIT = 1000;
    public static final int IO_THREADS = 2;

    // IO params.
    protected static Configuration CONFIGURATION;
    protected static Path FILE_PATH;
    protected static FileSystem FILESYSTEM;
    protected static ExecutorService IO_EXECUTOR;
    final ExecutionContext context = new ExecutionContext();

    // for compression
    protected CompressionKind compressionKind;
    protected int compressionSize;

    // stripe-level and file-level meta
    protected TreeMap<Integer, StripeInformation> stripeInformationMap;
    protected TypeDescription fileSchema;
    protected OrcFile.WriterVersion version;
    protected ReaderEncryption encryption;
    protected boolean ignoreNonUtf8BloomFilter;
    protected int maxBufferSize;
    protected int indexStride;
    protected int maxDiskRangeChunkLimit;
    protected long maxMergeDistance;

    // need preheating
    protected PreheatFileMeta preheatFileMeta;
    protected OrcTail orcTail;

    protected List<DataType> inputTypes;
    protected SortedMap<Integer, OrcProto.ColumnEncoding[]> encodingMap;

    protected MemoryAllocatorCtx memoryAllocatorCtx;

    protected static String getFileFromClasspath(String name) {
        URL url = ClassLoader.getSystemResource(name);
        if (url == null) {
            throw new IllegalArgumentException("Could not find " + name);
        }
        return url.getPath();
    }

    protected static PreheatFileMeta preheat() throws IOException {
        ORCMetaReader metaReader = null;
        try {
            metaReader = ORCMetaReader.create(CONFIGURATION, FILESYSTEM);
            PreheatFileMeta preheatFileMeta = metaReader.preheat(FILE_PATH);

            return preheatFileMeta;
        } finally {
            metaReader.close();
        }
    }

    protected static void print(VectorizedRowBatch batch) {
        for (int row = 0; row < batch.size; row++) {
            StringBuilder builder = new StringBuilder();

            for (int columnIndex = 0; columnIndex < batch.cols.length; columnIndex++) {
                ColumnVector vector = batch.cols[columnIndex];

                if (vector.isNull[row]) {
                    builder.append("null, ");
                } else {
                    vector.stringifyValue(builder, row);
                    builder.append(", ");
                }
            }
            System.out.println(builder);
        }
    }

    protected abstract void check(Block block, VectorizedRowBatch batch, int targetColumnId);

    @Before
    public void prepareParams() throws IOException {
        preheatFileMeta = preheat();

        orcTail = preheatFileMeta.getPreheatTail();

        // compression info
        compressionKind = orcTail.getCompressionKind();
        compressionSize = orcTail.getCompressionBufferSize();

        // get the mapping from stripe id to stripe information.
        List<StripeInformation> stripeInformationList = orcTail.getStripes();
        stripeInformationMap = stripeInformationList.stream().collect(Collectors.toMap(
            stripe -> (int) stripe.getStripeId(),
            stripe -> stripe,
            (s1, s2) -> s1,
            () -> new TreeMap<>()
        ));

        fileSchema = orcTail.getSchema();
        version = orcTail.getWriterVersion();

        // encryption info for reading.
        OrcProto.Footer footer = orcTail.getFooter();
        encryption = new ReaderEncryption(footer, fileSchema,
            orcTail.getStripeStatisticsOffset(), orcTail.getTailBuffer(), stripeInformationList,
            null, CONFIGURATION);

        // should the reader ignore the obsolete non-UTF8 bloom filters.
        ignoreNonUtf8BloomFilter = OrcConf.IGNORE_NON_UTF8_BLOOM_FILTERS.getBoolean(CONFIGURATION);

        // max buffer size in single IO task.
        maxBufferSize = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(CONFIGURATION);

        // the max row count in one row group.
        indexStride = orcTail.getFooter().getRowIndexStride();

        maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(CONFIGURATION);
        maxMergeDistance = OrcConf.MAX_MERGE_DISTANCE.getLong(CONFIGURATION);

        boolean[] columnIncluded = new boolean[fileSchema.getMaximumId() + 1];
        Arrays.fill(columnIncluded, true);
        encodingMap = stripeInformationList.stream().collect(Collectors.toMap(
            stripe -> (int) stripe.getStripeId(),
            stripe -> StaticStripePlanner.buildEncodings(
                encryption,
                columnIncluded,
                preheatFileMeta.getStripeFooter((int) stripe.getStripeId())),
            (s1, s2) -> s1,
            () -> new TreeMap<>()
        ));

        inputTypes = createInputTypes();

        ExecutionContext context = new ExecutionContext();
        context.setTraceId("mock_trace_id");
        if (context.getMemoryPool() == null) {
            context.setMemoryPool(MemoryManager.getInstance().createQueryMemoryPool(
                WorkloadUtil.isApWorkload(
                    context.getWorkloadType()), context.getTraceId(), context.getExtraCmds()));
        }

        MemoryPool memoryPool = MemoryPoolUtils
            .createOperatorTmpTablePool("ColumnarScanExec@" + System.identityHashCode(this),
                context.getMemoryPool());
        this.memoryAllocatorCtx = memoryPool.getMemoryAllocatorCtx();
    }

    protected abstract List<DataType> createInputTypes();

    protected void doTest(int stripeId, int columnId,
                          boolean[] columnIncluded,
                          boolean[] rowGroupIncluded,
                          List<ScanTestBase.BlockLocation> locationList)
        throws IOException {

        final StripeInformation stripeInformation = stripeInformationMap.get(stripeId);
        final OrcIndex orcIndex = preheatFileMeta.getOrcIndex(
            stripeInformation.getStripeId()
        );
        final OrcProto.ColumnEncoding[] encodings = encodingMap.get(stripeId);

        // metrics named ColumnReaderTest
        RuntimeMetrics runtimeMetrics = RuntimeMetrics.create("Test");
        // Add parent metrics node
        runtimeMetrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_MEMORY,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);
        runtimeMetrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_TIMER,
            null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);
        runtimeMetrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_BYTES_RANGE,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

        runtimeMetrics.addDerivedCounter(ColumnReader.COLUMN_READER_MEMORY,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);
        runtimeMetrics.addDerivedCounter(ColumnReader.COLUMN_READER_TIMER,
            null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);

        runtimeMetrics.addDerivedCounter(LogicalRowGroup.BLOCK_LOAD_TIMER,
            null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);
        runtimeMetrics.addDerivedCounter(LogicalRowGroup.BLOCK_MEMORY_COUNTER,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

        StripeLoader stripeLoader = null;
        ColumnReader columnReader = null;
        try {
            stripeLoader = createStripeLoader(stripeId, columnIncluded, runtimeMetrics);
            stripeLoader.open();

            CompletableFuture<Map<StreamName, InStream>> loadFuture =
                stripeLoader.load(columnId, rowGroupIncluded);

            // Use async mode and wait for completion of stripe loader.
            columnReader = createColumnReader(columnId, orcIndex, runtimeMetrics, stripeLoader, encodings);
            columnReader.open(loadFuture, false, rowGroupIncluded);

            for (ScanTestBase.BlockLocation location : locationList) {
                // block builder matched with raw orc data.
                Block block = allocateBlock(columnReader, inputTypes.get(columnId - 1), false,
                    TimeZone.getDefault(), encodings[columnId], location.positionCount);

                // move index of column-reader and start reading from this index.
                columnReader.startAt(location.rowGroupId, location.startPosition);
                columnReader.next((RandomAccessBlock) block, location.positionCount);

                // Check block
                doValidate(block, columnId, stripeId, orcTail.getStripes(), location);
            }

            System.out.println(runtimeMetrics.reportAll());
        } finally {
            if (stripeLoader != null) {
                stripeLoader.close();
            }
            if (columnReader != null) {
                columnReader.close();
            }
        }
    }

    protected Block allocateBlock(ColumnReader columnReader, DataType inputType, boolean compatible,
                                  TimeZone timeZone, OrcProto.ColumnEncoding encoding,
                                  int positionCount) {
        // NOTE: we need more type-specified implementations
        switch (inputType.fieldType()) {
        case MYSQL_TYPE_LONGLONG: {
            if (!inputType.isUnsigned()) {
                Preconditions.checkArgument(columnReader instanceof LongColumnReader);
                // for signed bigint.
                return new LongBlock(inputType, positionCount);
            } else {
                Preconditions.checkArgument(columnReader instanceof UnsignedLongColumnReader);
                // for unsigned bigint
                return new ULongBlock(inputType, positionCount);
            }
        }
        case MYSQL_TYPE_LONG: {
            if (!inputType.isUnsigned()) {
                Preconditions.checkArgument(columnReader instanceof IntegerColumnReader);
                // for signed int
                return new IntegerBlock(inputType, positionCount);
            } else {
                Preconditions.checkArgument(columnReader instanceof LongColumnReader);
                // for unsigned int
                return new LongBlock(inputType, positionCount);
            }
        }
        case MYSQL_TYPE_SHORT: {
            if (!inputType.isUnsigned()) {
                Preconditions.checkArgument(columnReader instanceof ShortColumnReader);
                // for signed short
                return new ShortBlock(inputType, positionCount);
            } else {
                Preconditions.checkArgument(columnReader instanceof IntegerColumnReader);
                // for unsigned short
                return new IntegerBlock(inputType, positionCount);
            }
        }
        case MYSQL_TYPE_INT24: {
            Preconditions.checkArgument(columnReader instanceof IntegerColumnReader);
            return new IntegerBlock(inputType, positionCount);
        }
        case MYSQL_TYPE_TINY: {
            if (!inputType.isUnsigned()) {
                Preconditions.checkArgument(columnReader instanceof ByteColumnReader);
                // for signed tiny
                return new ByteBlock(inputType, positionCount);
            } else {
                Preconditions.checkArgument(columnReader instanceof ShortColumnReader);
                // for unsigned tiny
                return new ShortBlock(inputType, positionCount);
            }
        }
        case MYSQL_TYPE_FLOAT: {
            Preconditions.checkArgument(columnReader instanceof FloatColumnReader);
            return new FloatBlock(inputType, positionCount);
        }
        case MYSQL_TYPE_DOUBLE: {
            Preconditions.checkArgument(columnReader instanceof DoubleColumnReader);
            return new DoubleBlock(inputType, positionCount);
        }
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2: {
            Preconditions.checkArgument(columnReader instanceof TimestampColumnReader);
            return new TimestampBlock(inputType, positionCount, timeZone);
        }
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2: {
            Preconditions.checkArgument(columnReader instanceof LongColumnReader);
            return new TimestampBlock(inputType, positionCount, timeZone);
        }
        case MYSQL_TYPE_YEAR: {
            Preconditions.checkArgument(columnReader instanceof LongColumnReader);
            return new LongBlock(inputType, positionCount);
        }
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE: {
            Preconditions.checkArgument(columnReader instanceof PackedTimeColumnReader);
            return new DateBlock(positionCount, timeZone);
        }
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2: {
            Preconditions.checkArgument(columnReader instanceof PackedTimeColumnReader);
            // for date
            return new TimeBlock(inputType, positionCount, timeZone);
        }
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL: {
            if (columnReader instanceof LongColumnReader) {
                return new DecimalBlock(inputType, positionCount, true);
            } else {
                return new DecimalBlock(inputType, positionCount, false);
            }
        }
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VARCHAR: {
            if (inputType instanceof BinaryType) {
                return new ByteArrayBlock(positionCount);
            } else if (inputType instanceof SliceType) {
                switch (encoding.getKind()) {
                case DIRECT:
                case DIRECT_V2: {
                    Preconditions.checkArgument(columnReader instanceof DirectVarcharColumnReader);
                    // For column in direct encoding
                    return new SliceBlock((SliceType) inputType, positionCount, compatible, false);
                }
                case DICTIONARY:
                case DICTIONARY_V2:
                    boolean enableSliceDict =
                        context.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_SLICE_DICT);

                    Preconditions.checkArgument(columnReader instanceof DictionaryVarcharColumnReader);
                    // for dictionary encoding
                    return new SliceBlock((SliceType) inputType, positionCount, compatible, enableSliceDict);
                default:
                    throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
                }
            } else {
                throw new UnsupportedOperationException("String type not supported: " + inputType.getClass().getName());
            }
        }
        case MYSQL_TYPE_ENUM: {
            switch (encoding.getKind()) {
            case DIRECT:
            case DIRECT_V2: {
                Preconditions.checkArgument(columnReader instanceof DirectEnumColumnReader);
                // For column in direct encoding
                return new EnumBlock(positionCount, ((EnumType) inputType).getEnumValues());
            }
            case DICTIONARY:
            case DICTIONARY_V2:
                Preconditions.checkArgument(columnReader instanceof DictionaryEnumColumnReader);
                // for dictionary encoding
                return new EnumBlock(positionCount, ((EnumType) inputType).getEnumValues());
            default:
                throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
            }
        }
        case MYSQL_TYPE_JSON: {
            Preconditions.checkArgument(columnReader instanceof DirectJsonColumnReader);
            return new StringBlock(inputType, positionCount);
        }
        case MYSQL_TYPE_BIT: {
            if (inputType instanceof BigBitType) {
                Preconditions.checkArgument(columnReader instanceof BigBitColumnReader);
                return new BigIntegerBlock(positionCount);
            } else {
                Preconditions.checkArgument(columnReader instanceof IntegerColumnReader);
                return new IntegerBlock(inputType, positionCount);
            }
        }
        case MYSQL_TYPE_BLOB: {
            // For column in both direct encoding and dictionary encoding
            return new BlobBlock(positionCount);
        }
        default:
        }
        throw GeneralUtil.nestedException("Unsupported input-type " + inputType);
    }

    @NotNull
    protected ColumnReader createColumnReader(int colId, OrcIndex orcIndex, RuntimeMetrics metrics,
                                              StripeLoader stripeLoader,
                                              OrcProto.ColumnEncoding[] encodings) {

        // Build column readers according to type-description and encoding kind.
        OrcProto.ColumnEncoding encoding = encodings[colId];
        Preconditions.checkNotNull(encoding);

        // NOTE: we need more type-specified implementations
        TypeDescription typeDescription = fileSchema.getChildren().get(colId - 1);
        DataType inputType = inputTypes.get(colId - 1);
        switch (inputType.fieldType()) {
        case MYSQL_TYPE_LONGLONG: {
            if (!inputType.isUnsigned()) {
                // for signed bigint.
                ColumnReader columnReader =
                    new LongColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(), indexStride,
                        true);
                return columnReader;
            } else {
                // for unsigned bigint
                ColumnReader columnReader =
                    new UnsignedLongColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                        indexStride, true);
                return columnReader;
            }
        }
        case MYSQL_TYPE_SHORT: {
            if (!inputType.isUnsigned()) {
                // for signed short
                ColumnReader columnReader =
                    new ShortColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                        indexStride, true);
                return columnReader;
            } else {
                // for unsigned short
                ColumnReader columnReader =
                    new IntegerColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                        indexStride, true);
                return columnReader;
            }
        }
        case MYSQL_TYPE_INT24: {
            // for signed/unsigned unsigned mediumint
            ColumnReader columnReader =
                new IntegerColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                    indexStride, true);
            return columnReader;
        }
        case MYSQL_TYPE_TINY: {
            if (!inputType.isUnsigned()) {
                // for signed tiny
                ColumnReader columnReader =
                    new ByteColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                        indexStride, true);
                return columnReader;
            } else {
                // for unsigned tiny
                ColumnReader columnReader =
                    new ShortColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                        indexStride, true);
                return columnReader;
            }
        }
        case MYSQL_TYPE_LONG: {
            if (!inputType.isUnsigned()) {
                // for signed int
                ColumnReader columnReader =
                    new IntegerColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                        indexStride, true);
                return columnReader;
            } else {
                // for unsigned int
                ColumnReader columnReader =
                    new LongColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                        indexStride, true);
                return columnReader;
            }
        }
        case MYSQL_TYPE_FLOAT: {
            ColumnReader columnReader =
                new FloatColumnReader(colId, true, stripeLoader, orcIndex, metrics, indexStride,
                    true);
            return columnReader;
        }
        case MYSQL_TYPE_DOUBLE: {
            ColumnReader columnReader =
                new DoubleColumnReader(colId, true, stripeLoader, orcIndex, metrics, indexStride,
                    true);
            return columnReader;
        }
        case MYSQL_TYPE_YEAR:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_DATETIME2: {
            ColumnReader columnReader =
                new LongColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(), indexStride,
                    true);
            return columnReader;
        }
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIMESTAMP2: {
            ColumnReader columnReader =
                new TimestampColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                    indexStride,
                    true, context);
            return columnReader;
        }
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_NEWDATE:
        case MYSQL_TYPE_TIME:
        case MYSQL_TYPE_TIME2: {
            // for date
            ColumnReader columnReader =
                new PackedTimeColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                    indexStride, true);
            return columnReader;
        }
        case MYSQL_TYPE_DECIMAL:
        case MYSQL_TYPE_NEWDECIMAL: {
            if (TypeUtils.isDecimal64Precision(inputType.getPrecision())) {
                ColumnReader columnReader =
                    new LongColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                        indexStride, true);
                return columnReader;
            } else {
                ColumnReader columnReader =
                    new DecimalColumnReader(colId, true, stripeLoader, orcIndex, metrics, true);
                return columnReader;
            }
        }
        case MYSQL_TYPE_VAR_STRING:
        case MYSQL_TYPE_STRING:
        case MYSQL_TYPE_VARCHAR: {
            if (inputType instanceof BinaryType) {
                switch (encoding.getKind()) {
                case DIRECT:
                case DIRECT_V2: {
                    // For column in direct encoding
                    return new DirectBinaryColumnReader(colId, true, stripeLoader, orcIndex, metrics, indexStride,
                        true);
                }
                case DICTIONARY:
                case DICTIONARY_V2:
                    // for dictionary encoding
                    return new DictionaryBinaryColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding,
                        indexStride, true);
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
                    return new DirectVarcharColumnReader(colId, true, stripeLoader, orcIndex, metrics, indexStride,
                        true);
                }
                case DICTIONARY:
                case DICTIONARY_V2:
                    // for dictionary encoding
                    return new DictionaryVarcharColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding,
                        indexStride, true, enableSliceDict);
                default:
                    throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
                }
            } else {
                throw new UnsupportedOperationException("String type not supported: " + inputType.getClass().getName());
            }
        }
        case MYSQL_TYPE_ENUM: {
            ColumnReader columnReader =
                new DirectEnumColumnReader(colId, true, stripeLoader, orcIndex, metrics, indexStride,
                    true, inputType);
            return columnReader;
        }
        case MYSQL_TYPE_JSON: {
            switch (encoding.getKind()) {
            case DIRECT:
            case DIRECT_V2: {
                // For column in direct encoding
                return new DirectJsonColumnReader(colId, true, stripeLoader, orcIndex, metrics, indexStride,
                    true, inputType);
            }
            case DICTIONARY:
            case DICTIONARY_V2:
                // for dictionary encoding
                return new DictionaryJsonColumnReader(colId, true, stripeLoader, orcIndex, metrics,
                    encoding, indexStride, true);
            default:
                throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
            }
        }
        case MYSQL_TYPE_BIT: {
            if (inputType instanceof BigBitType) {
                ColumnReader columnReader =
                    new BigBitColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                        indexStride, true);
                return columnReader;
            } else {
                // for bit
                ColumnReader columnReader =
                    new IntegerColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding.getKind(),
                        indexStride, true);
                return columnReader;
            }
        }
        case MYSQL_TYPE_BLOB: {
            switch (encoding.getKind()) {
            case DIRECT:
            case DIRECT_V2: {
                // For column in direct encoding
                ColumnReader columnReader =
                    new DirectBlobColumnReader(colId, true, stripeLoader, orcIndex, metrics, indexStride,
                        true);
                return columnReader;
            }
            case DICTIONARY:
            case DICTIONARY_V2:
                // for dictionary encoding
                ColumnReader columnReader =
                    new DictionaryBlobColumnReader(colId, true, stripeLoader, orcIndex, metrics, encoding,
                        indexStride, true);
                return columnReader;
            default:
                throw GeneralUtil.nestedException("Unsupported encoding " + encoding.getKind());
            }
        }

        default:
            throw GeneralUtil.nestedException("Unsupported type-description " + typeDescription);
        }
    }

    protected StripeLoader createStripeLoader(int stripeId, boolean[] columnIncluded, RuntimeMetrics runtimeMetrics) {
        StripeLoader stripeLoader;

        OrcProto.ColumnEncoding[] encodings = StaticStripePlanner.buildEncodings(
            encryption, columnIncluded, preheatFileMeta.getStripeFooter(stripeId)
        );

        stripeLoader = new AsyncStripeLoader(
            IO_EXECUTOR,
            FILESYSTEM,
            CONFIGURATION,
            FILE_PATH,
            columnIncluded,
            compressionSize,
            compressionKind,
            preheatFileMeta,
            stripeInformationMap.get(stripeId),
            fileSchema,
            version,
            encryption,
            encodings, ignoreNonUtf8BloomFilter,
            maxBufferSize,
            maxDiskRangeChunkLimit, maxMergeDistance, runtimeMetrics,
            true, memoryAllocatorCtx);
        return stripeLoader;
    }

    protected void doValidate(
        Block targetBlock,
        int targetColumnId,
        int stripeId,
        List<StripeInformation> stripeInformationList,
        ScanTestBase.BlockLocation location) throws IOException {
        StripeInformation stripeInformation = stripeInformationList.get(stripeId);

        // count the total rows before this stripe.
        int stripeStartRows = 0;
        for (int i = 0; i < stripeId; i++) {
            stripeStartRows += stripeInformationList.get(i).getNumberOfRows();
        }

        Path path = new Path(getFileFromClasspath
            (getOrcFilename()));

        Reader.Options options = new Reader.Options(CONFIGURATION).schema(fileSchema)
            .range(stripeInformation.getOffset(), stripeInformation.getLength());

        try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(CONFIGURATION));
            RecordReader rows = reader.rows(options)) {

            // count start row position in given row group.
            int startPositionInGroup = stripeStartRows + location.rowGroupId * indexStride;
            rows.seekToRow(startPositionInGroup + location.startPosition);

            // seek and read values with position count.
            VectorizedRowBatch batch = fileSchema.createRowBatch(location.positionCount);
            rows.nextBatch(batch);

            check(targetBlock, batch, targetColumnId);
        }
    }

    protected abstract String getOrcFilename();

    @NotNull
    protected boolean[] fromRowGroupIds(int stripeId, int[] rowGroupIds) {
        return fromRowGroupIds(stripeId, rowGroupIds, false);
    }

    @NotNull
    protected boolean[] fromRowGroupIds(int stripeId, int[] rowGroupIds, boolean reverse) {
        final StripeInformation stripeInformation = stripeInformationMap.get(stripeId);
        final int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);

        // NOTE: the columnId is start from 0 (struct column) and the 1st is the first actual column.
        final boolean[] rowGroupIncluded = new boolean[groupsInStripe];
        Arrays.fill(rowGroupIncluded, reverse);

        for (int rowGroupId : rowGroupIds) {
            rowGroupIncluded[rowGroupId] = !reverse;
        }
        return rowGroupIncluded;
    }

}
