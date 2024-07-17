package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.executor.operator.scan.impl.AsyncStripeLoader;
import com.alibaba.polardbx.executor.operator.scan.impl.PreheatFileMeta;
import com.alibaba.polardbx.executor.operator.scan.impl.StaticStripePlanner;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileAccumulatorType;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileUnit;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRangeList;
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
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.StreamName;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class StripeLoaderTest {
    public static final int DEFAULT_CHUNK_LIMIT = 1000;
    public static final int IO_THREADS = 2;
    private static final TypeDescription SCHEMA = TypeDescription.createStruct();
    private static final int RG_INDEX = 10000;
    public static final long STRIPE_SIZE_IN_MB = 8L;
    public static final String TEST_ORC_FILE_NAME = "7bdafb89qwb.orc";

    // IO params.
    private static Configuration CONFIGURATION;
    private static Path FILE_PATH;
    private static FileSystem FILESYSTEM;
    private static ExecutorService IO_EXECUTOR;

    static {
        // 4 col = 1 pk + 1 bigint + 2 varchar
        SCHEMA.addField("pk", TypeDescription.createLong());
        SCHEMA.addField("i0", TypeDescription.createLong());
        SCHEMA.addField("order_id", TypeDescription.createString());
        SCHEMA.addField("v0", TypeDescription.createString());
    }

    // for compression
    private CompressionKind compressionKind;
    private int compressionSize;

    // stripe-level and file-level meta
    private TreeMap<Integer, StripeInformation> stripeInformationMap;
    private TypeDescription fileSchema;
    private OrcFile.WriterVersion version;
    private ReaderEncryption encryption;
    private boolean ignoreNonUtf8BloomFilter;
    private int maxBufferSize;
    private int indexStride;
    private int maxDiskRangeChunkLimit;
    private long maxMergeDistance;

    // need preheating
    private PreheatFileMeta preheatFileMeta;
    private MemoryAllocatorCtx memoryAllocatorCtx;

    private static String getFileFromClasspath(String name) {
        URL url = ClassLoader.getSystemResource(name);
        if (url == null) {
            throw new IllegalArgumentException("Could not find " + name);
        }
        return url.getPath();
    }

    private static PreheatFileMeta preheat() throws IOException {
        ORCMetaReader metaReader = null;
        try {
            metaReader = ORCMetaReader.create(CONFIGURATION, FILESYSTEM);
            PreheatFileMeta preheatFileMeta = metaReader.preheat(FILE_PATH);

            return preheatFileMeta;
        } finally {
            metaReader.close();
        }

    }

    @BeforeClass
    public static void prepareStaticParams() throws IOException {
        IO_EXECUTOR = Executors.newFixedThreadPool(IO_THREADS);

        CONFIGURATION = new Configuration();
        OrcConf.ROW_INDEX_STRIDE.setInt(CONFIGURATION, 10000);
        OrcConf.MAX_MERGE_DISTANCE.setLong(CONFIGURATION, 64L * 1024); // 256KB
        OrcConf.USE_ZEROCOPY.setBoolean(CONFIGURATION, true);
        OrcConf.STRIPE_SIZE.setLong(CONFIGURATION, STRIPE_SIZE_IN_MB * 1024 * 1024);

        FILE_PATH = new Path(
            getFileFromClasspath(TEST_ORC_FILE_NAME));

        FILESYSTEM = FileSystem.get(
            FILE_PATH.toUri(), CONFIGURATION
        );
    }

    @Before
    public void prepareParams() throws IOException {
        preheatFileMeta = preheat();

        OrcTail orcTail = preheatFileMeta.getPreheatTail();

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
        indexStride = OrcConf.ROW_INDEX_STRIDE.getInt(CONFIGURATION);

        maxDiskRangeChunkLimit = OrcConf.ORC_MAX_DISK_RANGE_CHUNK_LIMIT.getInt(CONFIGURATION);
        maxMergeDistance = OrcConf.MAX_MERGE_DISTANCE.getLong(CONFIGURATION);

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

    //    Meta in stripe 0:
    //    Stream: column 0 section ROW_INDEX start: 3 length 17
    //    Stream: column 1 section ROW_INDEX start: 20 length 514
    //    Stream: column 2 section ROW_INDEX start: 534 length 871
    //    Stream: column 3 section ROW_INDEX start: 1405 length 1320
    //    Stream: column 4 section ROW_INDEX start: 2725 length 1303
    //    Stream: column 1 section DATA start: 4028 length 629
    //    Stream: column 2 section PRESENT start: 4657 length 21028
    //    Stream: column 2 section DATA start: 25685 length 1644043
    //    Stream: column 3 section PRESENT start: 1669728 length 22105
    //    Stream: column 3 section DATA start: 1691833 length 2233904
    //    Stream: column 3 section LENGTH start: 3925737 length 79666
    //    Stream: column 4 section PRESENT start: 4005403 length 22148
    //    Stream: column 4 section DATA start: 4027551 length 2230890
    //    Stream: column 4 section LENGTH start: 6258441 length 79461

    // data: stripe=0, column=1, rg={0}
    // dataRange: [4028, 4657)
    // compression: ZLib, 629 bytes
    // streams: DATA

    // Test empty row-groups
    @Test
    public void testEmpty() throws IOException, ExecutionException, InterruptedException {
        final int columnId = 1;
        final int stripeId = 0;
        final StripeInformation stripeInformation = stripeInformationMap.get(stripeId);
        final int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);

        // NOTE: the columnId is start from 0 (struct column) and the 1st is the first actual column.
        final boolean[] columnIncluded = {true, true, true, false, false};
        final boolean[] rowGroupIncluded = new boolean[groupsInStripe];

        Arrays.fill(rowGroupIncluded, false);

        StripeLoader stripeLoader = null;

        RuntimeMetrics runtimeMetrics = RuntimeMetrics.create("StripeLoaderTest");
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

        OrcProto.ColumnEncoding[] encodings = StaticStripePlanner.buildEncodings(
            encryption, columnIncluded, preheatFileMeta.getStripeFooter(stripeId)
        );

        try {
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

            stripeLoader.open();

            CompletableFuture<Map<StreamName, InStream>> loadFuture =
                stripeLoader.load(columnId, rowGroupIncluded);

            // data range[4028, 4657)
            // {column 1 kind DATA=uncompressed stream column 1 kind DATA
            // position: 0 length: 629 range: 0 offset: 4028 position: 0 limit: 629}
            Map<StreamName, InStream> result = loadFuture.get();

            Assert.assertTrue(result.size() == 0);
            StreamName streamName = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
            InStream inStream = result.get(streamName);
            Assert.assertNull(inStream);

            result.forEach((k, v) -> {
                System.out.println(k + ", " + v);
            });

            String report = runtimeMetrics.reportAll();
            System.out.println(report);
        } finally {
            if (stripeLoader != null) {
                stripeLoader.close();
            }
        }
    }

    @Test
    public void test1() throws IOException, ExecutionException, InterruptedException {
        final int columnId = 1;
        final int stripeId = 0;
        final StripeInformation stripeInformation = stripeInformationMap.get(stripeId);
        final int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);

        // NOTE: the columnId is start from 0 (struct column) and the 1st is the first actual column.
        final boolean[] columnIncluded = {true, true, true, false, false};
        final boolean[] rowGroupIncluded = new boolean[groupsInStripe];

        Arrays.fill(rowGroupIncluded, false);
        rowGroupIncluded[0] = true;

        StripeLoader stripeLoader = null;

        RuntimeMetrics runtimeMetrics = RuntimeMetrics.create("StripeLoaderTest");
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

        OrcProto.ColumnEncoding[] encodings = StaticStripePlanner.buildEncodings(
            encryption, columnIncluded, preheatFileMeta.getStripeFooter(stripeId)
        );

        try {
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

            stripeLoader.open();

            CompletableFuture<Map<StreamName, InStream>> loadFuture =
                stripeLoader.load(columnId, rowGroupIncluded);

            // data range[4028, 4657)
            // {column 1 kind DATA=uncompressed stream column 1 kind DATA
            // position: 0 length: 629 range: 0 offset: 4028 position: 0 limit: 629}
            Map<StreamName, InStream> result = loadFuture.get();

            Assert.assertTrue(result.size() == 1);
            StreamName streamName = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
            InStream inStream = result.get(streamName);
            Assert.assertNotNull(inStream);

            result.forEach((k, v) -> {
                System.out.println(k + ", " + v);
            });

            String report = runtimeMetrics.reportAll();
            System.out.println(report);
        } finally {
            if (stripeLoader != null) {
                stripeLoader.close();
            }
        }
    }

    @Test
    public void test2() throws IOException, ExecutionException, InterruptedException {
        // The total data stream of column 1 (pk column) will be fetched at once
        // no matter how many row groups are selected.
        // Because the length of total stream is less than the MAX_MERGE_DISTANCE

        final int columnId = 2;
        final int stripeId = 1;
        final StripeInformation stripeInformation = stripeInformationMap.get(stripeId);
        final int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);

        // NOTE: the columnId is start from 0 (struct column) and the 1st is the first actual column.
        final boolean[] columnIncluded = {true, true, true, false, false};
        final boolean[] rowGroupIncluded = new boolean[groupsInStripe];

        Arrays.fill(rowGroupIncluded, false);
        rowGroupIncluded[0] = true;
        rowGroupIncluded[2] = true;
        rowGroupIncluded[3] = true;
        rowGroupIncluded[5] = true;
        rowGroupIncluded[10] = true;
        rowGroupIncluded[23] = true;

        System.out.println("groups in stripe = " + groupsInStripe);

        StripeLoader stripeLoader = null;

        RuntimeMetrics runtimeMetrics = RuntimeMetrics.create("StripeLoaderTest");
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

        OrcProto.ColumnEncoding[] encodings = StaticStripePlanner.buildEncodings(
            encryption, columnIncluded, preheatFileMeta.getStripeFooter(stripeId)
        );
        try {
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

            stripeLoader.open();

            CompletableFuture<Map<StreamName, InStream>> loadFuture =
                stripeLoader.load(columnId, rowGroupIncluded);

            Map<StreamName, InStream> result = loadFuture.get();

            Assert.assertTrue(result.size() == 2);

            // data stream
            StreamName dataStreamName = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
            InStream dataInStream = result.get(dataStreamName);
            Assert.assertNotNull(dataInStream);

            // present stream
            StreamName presentStreamName = new StreamName(columnId, OrcProto.Stream.Kind.PRESENT);
            InStream presentInStream = result.get(presentStreamName);
            Assert.assertNotNull(presentInStream);

            checkDiskRange(dataInStream, new String[] {
                "data range [6363557, 6625707), size: 262150 type: array-backed",
                "data range [6625707, 6887857), size: 262150 type: array-backed",
                "data range [6887857, 7018932), size: 131075 type: array-backed",
                "data range [7018932, 7281082), size: 262150 type: array-backed",
                "data range [7936457, 8010184), size: 73727 type: array-backed"
            });

            checkDiskRange(presentInStream, new String[] {
                "data range [6342586, 6363557), size: 20971 type: array-backed"
            });

            result.forEach((k, v) -> {
                System.out.println(k + ", " + v);
            });

            String report = runtimeMetrics.reportAll();
            System.out.println(report);
        } finally {
            if (stripeLoader != null) {
                stripeLoader.close();
            }
        }

    }

    @Test
    public void test3() throws IOException, ExecutionException, InterruptedException {
        // Stripe 2:
        //   Stripe: offset: 12677504 data: 6337314 rows: 240000 tail: 110 index: 4028
        //    Stream: column 0 section ROW_INDEX start: 12677504 length 17
        //    Stream: column 1 section ROW_INDEX start: 12677521 length 493
        //    Stream: column 2 section ROW_INDEX start: 12678014 length 873
        //    Stream: column 3 section ROW_INDEX start: 12678887 length 1323
        //    Stream: column 4 section ROW_INDEX start: 12680210 length 1322
        //    Stream: column 1 section DATA start: 12681532 length 566
        //    Stream: column 2 section PRESENT start: 12682098 length 21012
        //    Stream: column 2 section DATA start: 12703110 length 1646851
        //    Stream: column 3 section PRESENT start: 14349961 length 22078
        //    Stream: column 3 section DATA start: 14372039 length 2233754
        //    Stream: column 3 section LENGTH start: 16605793 length 79633
        //    Stream: column 4 section PRESENT start: 16685426 length 22115
        //    Stream: column 4 section DATA start: 16707541 length 2231725
        //    Stream: column 4 section LENGTH start: 18939266 length 79580
        //    Encoding column 0: DIRECT
        //    Encoding column 1: DIRECT_V2
        //    Encoding column 2: DIRECT_V2
        //    Encoding column 3: DIRECT_V2
        //    Encoding column 4: DIRECT_V2

        final int stripeId = 2;
        final StripeInformation stripeInformation = stripeInformationMap.get(stripeId);
        final int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);

        // NOTE: the columnId is start from 0 (struct column) and the 1st is the first actual column.
        final boolean[] columnIncluded = {true, true, true, true, true};
        // select cols: i0, order_id
        final List<Integer> selectedColumns = ImmutableList.of(2, 3);

        final Map<Integer, boolean[]> matrix = new TreeMap<>();

        // put row group for column 2:
        boolean[] rowGroupIncluded = new boolean[groupsInStripe];
        Arrays.fill(rowGroupIncluded, false);
        rowGroupIncluded[0] = true;
        rowGroupIncluded[1] = true;
        rowGroupIncluded[4] = true;
        rowGroupIncluded[13] = true;
        rowGroupIncluded[15] = true;
        rowGroupIncluded[16] = true;
        rowGroupIncluded[18] = true;
        matrix.put(2, rowGroupIncluded);

        // put row group for column 3:
        rowGroupIncluded = new boolean[groupsInStripe];
        Arrays.fill(rowGroupIncluded, false);
        rowGroupIncluded[1] = true;
        rowGroupIncluded[2] = true;
        rowGroupIncluded[3] = true;
        rowGroupIncluded[12] = true;
        rowGroupIncluded[14] = true;
        rowGroupIncluded[15] = true;
        rowGroupIncluded[17] = true;
        matrix.put(3, rowGroupIncluded);

        System.out.println("groups in stripe = " + groupsInStripe);

        StripeLoader stripeLoader = null;

        RuntimeMetrics runtimeMetrics = RuntimeMetrics.create("StripeLoaderTest");
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

        OrcProto.ColumnEncoding[] encodings = StaticStripePlanner.buildEncodings(
            encryption, columnIncluded, preheatFileMeta.getStripeFooter(stripeId)
        );
        try {
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

            stripeLoader.open();

            CompletableFuture<Map<StreamName, InStream>> loadFuture =
                stripeLoader.load(selectedColumns, matrix);

            // column 2 kind DATA, compressed stream column 2 kind DATA position: 0 length: 1646851 range: 0 offset: 0 limit: 393225 range 0 = 12703110 to 13096335;  range 1 = 13096335 to 13227410;  range 2 = 13489560 to 13882785;  range 3 = 13882785 to 14013860;  range 4 = 14013860 to 14144935;  range 5 = 14349961 to 14372039;  range 6 = 14463453 to 14999852;  range 7 = 15468913 to 15822474;  range 8 = 15822474 to 16096652;  range 9 = 16096652 to 16279479;  range 10 = 16605793 to 16685426
            // column 2 kind PRESENT, compressed stream column 2 kind PRESENT position: 0 length: 21012 range: 0 offset: 0 limit: 21012 range 0 = 12682098 to 12703110;  range 1 = 12703110 to 13096335;  range 2 = 13096335 to 13227410;  range 3 = 13489560 to 13882785;  range 4 = 13882785 to 14013860;  range 5 = 14013860 to 14144935;  range 6 = 14349961 to 14372039;  range 7 = 14463453 to 14999852;  range 8 = 15468913 to 15822474;  range 9 = 15822474 to 16096652;  range 10 = 16096652 to 16279479;  range 11 = 16605793 to 16685426
            // column 3 kind DATA, compressed stream column 3 kind DATA position: 91414 length: 2233754 range: 0 offset: 0 limit: 536399 range 0 = 14463453 to 14999852;  range 1 = 15468913 to 15822474;  range 2 = 15822474 to 16096652;  range 3 = 16096652 to 16279479;  range 4 = 16605793 to 16685426
            // column 3 kind LENGTH, compressed stream column 3 kind LENGTH position: 0 length: 79633 range: 0 offset: 0 limit: 79633 range 0 = 16605793 to 16685426
            // column 3 kind PRESENT, compressed stream column 3 kind PRESENT position: 0 length: 22078 range: 0 offset: 0 limit: 22078 range 0 = 14349961 to 14372039;  range 1 = 14463453 to 14999852;  range 2 = 15468913 to 15822474;  range 3 = 15822474 to 16096652;  range 4 = 16096652 to 16279479;  range 5 = 16605793 to 16685426
            Map<StreamName, InStream> result = loadFuture.get();
            AtomicLong accumulator = new AtomicLong(0L);

            result.forEach((streamName, inStream) -> {
                System.out.println(streamName);

                for (DiskRangeList node = inStream.getBytes(); node != null; node = node.next) {
                    if (node.getOffset() >= inStream.getOffset()
                        && node.getEnd() <= inStream.getOffset() + inStream.getLength()) {
                        System.out.println(node);
                        accumulator.addAndGet(node.getLength());
                    }
                }
            });

            System.out.println("accumulator = " + accumulator.get());

            String report = runtimeMetrics.reportAll();
            System.out.println(report);

        } finally {
            if (stripeLoader != null) {
                stripeLoader.close();
            }
        }

    }

    private void checkDiskRange(InStream dataInStream, String[] expectedResult) {
        // check disk range
        int expectedResultIndex = 0;
        for (DiskRangeList node = dataInStream.getBytes(); node != null; node = node.next) {
            if (node.getOffset() >= dataInStream.getOffset()
                && node.getEnd() <= dataInStream.getOffset() + dataInStream.getLength()) {
                System.out.println(node);
                Assert.assertEquals(expectedResult[expectedResultIndex++], node.toString());
            }
        }
    }

    private void doTest(int stripeId, int columnId, boolean[] columnIncluded, boolean[] rowGroupIncluded)
        throws IOException, InterruptedException, ExecutionException {
        StripeLoader stripeLoader = null;

        RuntimeMetrics runtimeMetrics = RuntimeMetrics.create("StripeLoaderTest");
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

        OrcProto.ColumnEncoding[] encodings = StaticStripePlanner.buildEncodings(
            encryption, columnIncluded, preheatFileMeta.getStripeFooter(stripeId)
        );
        try {
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

            stripeLoader.open();

            CompletableFuture<Map<StreamName, InStream>> loadFuture =
                stripeLoader.load(columnId, rowGroupIncluded);

            // data range[4028, 4657)
            // {column 1 kind DATA=uncompressed stream column 1 kind DATA
            // position: 0 length: 629 range: 0 offset: 4028 position: 0 limit: 629}
            Map<StreamName, InStream> result = loadFuture.get();

            Assert.assertTrue(result.size() == 1);
            StreamName streamName = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
            InStream inStream = result.get(streamName);
            Assert.assertNotNull(inStream);

            result.forEach((k, v) -> {
                System.out.println(k + ", " + v);
            });

            String report = runtimeMetrics.reportAll();
            System.out.println(report);

            // result = {HashMap@2600}  size = 1
            // {StreamName@2602} "column 1 kind DATA" -> {InStream$CompressedStream@2618} "compressed stream column 1 kind DATA position: 0 length: 629 range: 0 offset: 0 limit: 629 range 0 = 4028 to 4657"
            //  key = {StreamName@2602} "column 1 kind DATA"
            //   column = 1 (0x1)
            //   encryption = null
            //   kind = {OrcProto$Stream$Kind@2627} "DATA"
            //  value = {InStream$CompressedStream@2618} "compressed stream column 1 kind DATA position: 0 length: 629 range: 0 offset: 0 limit: 629 range 0 = 4028 to 4657"
            //   bufferSize = 131072 (0x20000)
            //   uncompressed = null
            //   codec = {ZlibCodec@1834}
            //   compressed = {HeapByteBuffer@2623} "java.nio.HeapByteBuffer[pos=0 lim=629 cap=629]"
            //   currentRange = {BufferChunk@2604} "data range [4028, 4657), size: 629 type: array-backed"
            //   isUncompressedOriginal = false
            //   recyclables = {ConcurrentLinkedQueue@2624}  size = 0
            //   name = {StreamName@2602} "column 1 kind DATA"
            //   offset = 4028 (0xFBC)
            //   length = 629 (0x275)
            //   bytes = {BufferChunk@2604} "data range [4028, 4657), size: 629 type: array-backed"
            //   position = 0 (0x0)

        } finally {
            if (stripeLoader != null) {
                stripeLoader.close();
            }
        }
    }

    private static void print(VectorizedRowBatch batch) {
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

    @Test
    @Ignore
    public void test01() throws IOException {
//        final Configuration configuration = new Configuration();
//        OrcConf.ROW_INDEX_STRIDE.setInt(configuration, 10000);
//        OrcConf.MAX_MERGE_DISTANCE.setLong(configuration, 256L * 1024);
//        OrcConf.USE_ZEROCOPY.setBoolean(configuration, false);
//
//        final Path filePath = new Path(
//            getFileFromClasspath("7bdafb89qwb.orc"));
//
//        final FileSystem fileSystem = FileSystem.get(
//            filePath.toUri(), configuration
//        );

        ORCMetaReader metaReader = null;
        try {
            metaReader = ORCMetaReader.create(CONFIGURATION, FILESYSTEM);
            PreheatFileMeta preheatFileMeta = metaReader.preheat(FILE_PATH);

            System.out.println(preheatFileMeta.getPreheatStripes());
            System.out.println(preheatFileMeta.getPreheatTail());
            System.out.println(preheatFileMeta.getOrcIndex(0));
        } finally {
            metaReader.close();
        }

    }

    @Test
//    @Ignore
    public void test02() throws IOException {
        Path path = new Path(getFileFromClasspath
            (TEST_ORC_FILE_NAME));

        Reader.Options options = new Reader.Options(CONFIGURATION).schema(SCHEMA);
        try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(CONFIGURATION));
            RecordReader rows = reader.rows(options)) {

            VectorizedRowBatch batch = SCHEMA.createRowBatch(DEFAULT_CHUNK_LIMIT);

            rows.nextBatch(batch);
            rows.nextBatch(batch);

            print(batch);

//            int rowCount = 10000;
//
//            while (rows.nextBatch(batch) && rowCount >= 0) {
//                print(batch);
//                rowCount -= batch.size;
//            }
        }
    }
}
