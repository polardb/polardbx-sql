package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.archive.schemaevolution.ColumnMetaWithTs;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.operator.scan.impl.AsyncStripeLoader;
import com.alibaba.polardbx.executor.operator.scan.impl.PreheatFileMeta;
import com.alibaba.polardbx.executor.operator.scan.impl.StaticStripePlanner;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.FileMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ScanTestBase {

    protected MemoryAllocatorCtx memoryAllocatorCtx;

    // Mock for BlockLoader
    // It only contains the location info of BlockLoader.
    protected static class BlockLocation implements Comparable<BlockLocation> {
        final int rowGroupId;

        // position in row-group starting from 0
        final int startPosition;
        final int positionCount;

        public BlockLocation(int rowGroupId, int startPosition, int positionCount) {
            this.rowGroupId = rowGroupId;
            this.startPosition = startPosition;
            this.positionCount = positionCount;
        }

        @Override
        public int compareTo(@NotNull ScanTestBase.BlockLocation that) {
            if (that == null) {
                return 1;
            } else {
                return (rowGroupId != that.rowGroupId) ? (rowGroupId - that.rowGroupId)
                    : ((startPosition != that.startPosition) ? (startPosition - that.startPosition)
                    : ((positionCount != that.positionCount) ? (positionCount - that.positionCount) : 0));
            }
        }
    }

    public static final String TRACE_ID = "1d3hfeq89fda";
    public static final int DEFAULT_CHUNK_LIMIT = 1000;
    public static final int IO_THREADS = 2;
    public static final TypeDescription SCHEMA = TypeDescription.createStruct();
    public static final int RG_INDEX = 10000;
    public static final long STRIPE_SIZE_IN_MB = 8L;
    public static final String TEST_ORC_FILE_NAME = "7bdafb89qwb.orc";

    public static final int SEQUENCE_ID = 156428234;
    public static final int FILE_ID = 1;

    // IO params.
    protected static Configuration CONFIGURATION;
    protected static Path FILE_PATH;
    protected static FileSystem FILESYSTEM;
    protected static ExecutorService IO_EXECUTOR;

    protected static final List<ColumnMeta> COLUMN_METAS = new ArrayList<>();
    protected static final List<Integer> LOC_IN_ORC = new ArrayList<>();

    static {
        // 4 col = 1 pk + 1 bigint + 2 varchar
        SCHEMA.addField("pk", TypeDescription.createLong());
        SCHEMA.addField("i0", TypeDescription.createLong());
        SCHEMA.addField("order_id", TypeDescription.createString());
        SCHEMA.addField("v0", TypeDescription.createString());

        COLUMN_METAS.add(new ColumnMeta("t1", "pk", "pk", new Field(DataTypes.LongType)));
        COLUMN_METAS.add(new ColumnMeta("t1", "i0", "i0", new Field(DataTypes.LongType)));
        COLUMN_METAS.add(new ColumnMeta("t1", "order_id", "order_id", new Field(new VarcharType())));
        COLUMN_METAS.add(new ColumnMeta("t1", "v0", "v0", new Field(new VarcharType())));

        LOC_IN_ORC.add(1);
        LOC_IN_ORC.add(2);
        LOC_IN_ORC.add(3);
        LOC_IN_ORC.add(4);
    }

    // for compression
    protected CompressionKind compressionKind;
    protected int compressionSize;

    // stripe-level and file-level meta
    protected TreeMap<Integer, StripeInformation> stripeInformationMap;

    // The start row id of each stripe.
    protected Map<Integer, Long> startRowInStripeMap;
    protected TypeDescription fileSchema;
    protected OrcFile.WriterVersion version;
    protected ReaderEncryption encryption;
    protected boolean ignoreNonUtf8BloomFilter;
    protected int maxBufferSize;
    protected int indexStride;
    protected boolean enableDecimal64;
    protected int maxDiskRangeChunkLimit;
    protected long maxMergeDistance;

    // need preheating
    protected PreheatFileMeta preheatFileMeta;
    protected OrcTail orcTail;

    public static String getFileFromClasspath(String name) {
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

    @BeforeClass
    public static void prepareStaticParams() throws IOException {
        IO_EXECUTOR = Executors.newFixedThreadPool(IO_THREADS);

        CONFIGURATION = new Configuration();
        OrcConf.ROW_INDEX_STRIDE.setInt(CONFIGURATION, 10000);
        OrcConf.MAX_MERGE_DISTANCE.setLong(CONFIGURATION, 64L * 1024); // 64KB
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

        // Get the start rows of each stipe.
        startRowInStripeMap = new HashMap<>();
        long startRows = 0;
        for (int stripeId = 0; stripeId < stripeInformationList.size(); stripeId++) {
            StripeInformation stripeInformation = stripeInformationList.get(stripeId);
            startRowInStripeMap.put(stripeId, startRows);
            long nextStartRows = startRows + stripeInformation.getNumberOfRows();
            startRows = nextStartRows;
        }

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
        enableDecimal64 = OrcConf.ENABLE_DECIMAL_64.getBoolean(CONFIGURATION);

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

    @NotNull
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

    protected int getRowCount(StripeInformation stripeInformation, int groupId) {
        final long rowsInStripe = stripeInformation.getNumberOfRows();
        int groupsInStripe = (int) ((rowsInStripe + indexStride - 1) / indexStride);

        if (groupId != groupsInStripe - 1) {
            return indexStride;
        } else {
            return (int) (rowsInStripe - (groupsInStripe - 1) * indexStride);
        }
    }

    protected BlockCacheManager<Block> prepareCache(int stripeId, int[] columnIds, int[] cachedGroupIds)
        throws IOException {
        final StripeInformation stripeInformation = orcTail.getStripes().get(stripeId);

        final BlockCacheManager<Block> blockCacheManager = BlockCacheManager.getInstance();

        // for all cached row group ids, fill the generated blocks.
        for (int groupId : cachedGroupIds) {
            final int rowCountInGroup = getRowCount(stripeInformation, groupId);

            // collect block locations in this group.
            List<BlockLocation> locationList = new ArrayList<>();
            for (int startPosition = 0; startPosition < rowCountInGroup; startPosition += DEFAULT_CHUNK_LIMIT) {
                int positionCount = Math.min(DEFAULT_CHUNK_LIMIT, rowCountInGroup - startPosition);
                locationList.add(new BlockLocation(groupId, startPosition, positionCount));
            }

            // for each column id
            for (int columnId : columnIds) {
                // Create blocks from orc file with given locations.
                SortedMap<BlockLocation, Block> blocks = createBlock(
                    stripeId, orcTail.getStripes(),
                    locationList, columnId
                );

                // put all generated blocks into block cache manager.
                // The block in cache manager is visible
                // only in condition that the full blocks of one row-group is cached.
                blocks.forEach((location, block) ->
                    blockCacheManager.putCache(
                        block, DEFAULT_CHUNK_LIMIT, rowCountInGroup,
                        FILE_PATH,
                        stripeId, groupId, columnId,
                        location.startPosition, location.positionCount
                    )
                );
            }
        }

        for (int columnId : columnIds) {
            Map<Integer, SeekableIterator<Block>> cachedRowGroups =
                blockCacheManager.getCachedRowGroups(FILE_PATH, stripeId, columnId,
                    fromRowGroupIds(stripeId, cachedGroupIds));

//            cachedRowGroups.forEach((groupId, iterator) -> {
//                System.out.println("groupId: " + groupId);
//                while (iterator.hasNext()) {
//                    Block block = iterator.next();
//                    System.out.println("block position count = " + block.getPositionCount());
//                }
//            });
        }

        return blockCacheManager;
    }

    private SortedMap<BlockLocation, Block> createBlock(int stripeId,
                                                        List<StripeInformation> stripeInformationList,
                                                        List<BlockLocation> blockLocationList,
                                                        int targetColumnId) throws IOException {
        StripeInformation stripeInformation = stripeInformationList.get(stripeId);

        // count the total rows before this stripe.
        int stripeStartRows = 0;
        for (int i = 0; i < stripeId; i++) {
            stripeStartRows += stripeInformationList.get(i).getNumberOfRows();
        }

        Path path = new Path(getFileFromClasspath
            (TEST_ORC_FILE_NAME));

        Reader.Options options = new Reader.Options(CONFIGURATION).schema(SCHEMA)
            .range(stripeInformation.getOffset(), stripeInformation.getLength());

        // sorted by block-location.
        SortedMap<BlockLocation, Block> result = new TreeMap<>(BlockLocation::compareTo);
        try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(CONFIGURATION));
            RecordReader rows = reader.rows(options)) {

            for (BlockLocation location : blockLocationList) {

                // count start row position in given row group.
                int startPositionInGroup = stripeStartRows + location.rowGroupId * indexStride;
                rows.seekToRow(startPositionInGroup + location.startPosition);

                // seek and read values with position count.
                VectorizedRowBatch batch = SCHEMA.createRowBatch(location.positionCount);
                rows.nextBatch(batch);

                // NOTE: Only support long type until now.
                LongColumnVector vector = (LongColumnVector) batch.cols[targetColumnId - 1];

                // Build block and convert from column vector.
                BlockBuilder blockBuilder = BlockBuilders
                    .create(DataTypes.LongType, new ExecutionContext(), batch.size);
                for (int i = 0; i < batch.size; i++) {
                    if (vector.isNull != null && vector.isNull[i]) {
                        blockBuilder.appendNull();
                    } else {
                        blockBuilder.writeLong(vector.vector[i]);
                    }
                }
                Block block = blockBuilder.build();
                result.put(location, block);
            }

        }

        return result;
    }

    protected ColumnarManager mockColumnarManager = new ColumnarManager() {
        @Override
        public void reload() {

        }

        @Override
        public void reload(ReloadType type) {

        }

        @Override
        public long latestTso() {
            return 0;
        }

        @Override
        public void setLatestTso(long tso) {

        }

        @Override
        public void purge(long tso) {

        }

        @Override
        public Pair<List<FileMeta>, List<FileMeta>> findFiles(long tso, String logicalSchema, String logicalTable,
                                                              String partName) {
            return null;
        }

        @Override
        public Pair<List<String>, List<String>> findFileNames(long tso, String logicalSchema, String logicalTable,
                                                              String partName) {
            return null;
        }

        @Override
        public Iterator<Chunk> csvData(long tso, String csvFileName) {
            return null;
        }

        @Override
        public Iterator<Chunk> csvData(String csvFileName, long position) {
            return null;
        }

        @Override
        public Iterator<Chunk> flashbackCsvData(long tso, String csvFileName) {
            return null;
        }

        @Override
        public int fillSelection(String fileName, long tso, int[] selection, LongBlock positionBlock) {
            return 0;
        }

        @Override
        public int fillSelection(String fileName, long tso, int[] selection, LongColumnVector longColumnVector,
                                 int batchSize) {
            return 0;
        }

        @Override
        public Optional<String> fileNameOf(String logicalSchema, long tableId, String partName, int columnarFileId) {
            return Optional.empty();
        }

        @Override
        public FileMeta fileMetaOf(String fileName) {
            return null;
        }

        @Override
        public @NotNull List<Long> getColumnFieldIdList(long versionId, long tableId) {
            return null;
        }

        @Override
        public @NotNull List<ColumnMeta> getColumnMetas(long schemaTso, String logicalSchema, String logicalTable) {
            return null;
        }

        @Override
        public @NotNull List<ColumnMeta> getColumnMetas(long schemaTso, long tableId) {
            return null;
        }

        @Override
        public @NotNull Map<Long, Integer> getColumnIndex(long schemaTso, long tableId) {
            return null;
        }

        @Override
        public @NotNull ColumnMetaWithTs getInitColumnMeta(long tableId, long fieldId) {
            return null;
        }

        @Override
        public RoaringBitmap getDeleteBitMapOf(long tso, String fileName) {
            return null;
        }

        @Override
        public List<Integer> getPhysicalColumnIndexes(long tso, String fileName, List<Integer> columnIndexes) {
            return columnIndexes;
        }

        @Override
        public Map<Long, Integer> getPhysicalColumnIndexes(String fileName) {
            return null;
        }

        @Override
        public List<Integer> getSortKeyColumns(long tso, String logicalSchema, String logicalTable) {
            List<Integer> mockList = new ArrayList<>();
            mockList.add(0);
            return mockList;
        }

        @Override
        public int[] getPrimaryKeyColumns(String fileName) {
            // this is a mock for test
            return new int[] {1};
        }
    };
}
