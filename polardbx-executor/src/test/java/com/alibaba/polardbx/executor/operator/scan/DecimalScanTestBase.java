package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.charset.MySQLUnicodeUtils;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.operator.scan.impl.AsyncStripeLoader;
import com.alibaba.polardbx.executor.operator.scan.impl.PreheatFileMeta;
import com.alibaba.polardbx.executor.operator.scan.impl.StaticStripePlanner;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import com.alibaba.polardbx.optimizer.workload.WorkloadUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
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
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public abstract class DecimalScanTestBase {

    private MemoryAllocatorCtx memoryAllocatorCtx;

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
        public int compareTo(@NotNull DecimalScanTestBase.BlockLocation that) {
            if (that == null) {
                return 1;
            } else {
                return (rowGroupId != that.rowGroupId) ? (rowGroupId - that.rowGroupId)
                    : ((startPosition != that.startPosition) ? (startPosition - that.startPosition)
                    : ((positionCount != that.positionCount) ? (positionCount - that.positionCount) : 0));
            }
        }
    }

    public static final int DEFAULT_CHUNK_LIMIT = 1000;
    public static final int IO_THREADS = 2;
    public static final long STRIPE_SIZE_IN_MB = 8L;

    // IO params.
    protected static Configuration CONFIGURATION;
    protected static Path FILE_PATH;
    protected static FileSystem FILESYSTEM;
    protected static ExecutorService IO_EXECUTOR;

    public static TypeDescription SCHEMA;
    protected static List<DataType> INPUT_TYPES;

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

    @Before
    public void prepareParams() throws IOException {
        SCHEMA = TypeDescription.createStruct();
        INPUT_TYPES = new ArrayList<>();
        initSchema();

        IO_EXECUTOR = Executors.newFixedThreadPool(IO_THREADS);

        CONFIGURATION = new Configuration();
        OrcConf.ROW_INDEX_STRIDE.setInt(CONFIGURATION, 10000);
        OrcConf.MAX_MERGE_DISTANCE.setLong(CONFIGURATION, 64L * 1024); // 64KB
        OrcConf.USE_ZEROCOPY.setBoolean(CONFIGURATION, true);
        OrcConf.STRIPE_SIZE.setLong(CONFIGURATION, STRIPE_SIZE_IN_MB * 1024 * 1024);

        FILE_PATH = new Path(
            getFileFromClasspath(getOrcFileName()));

        FILESYSTEM = FileSystem.get(
            FILE_PATH.toUri(), CONFIGURATION
        );
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

    protected abstract void initSchema();

    abstract String getOrcFileName();

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
                        block, DEFAULT_CHUNK_LIMIT,
                        rowCountInGroup,
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
            (getOrcFileName()));

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

    protected static void check(DecimalBlock block, VectorizedRowBatch batch, int targetColumnId) {
        for (int row = 0; row < batch.size; row++) {
            ColumnVector vector = batch.cols[1];
            int idx = row;
            if (vector.isRepeating) {
                idx = 0;
            }
            if (vector.isNull[idx]) {
                // check null
                Assert.assertTrue(block.isNull(row));
            } else {
                Assert.assertTrue(vector instanceof BytesColumnVector);
                BytesColumnVector bytesColumnVector = (BytesColumnVector) vector;
                int pos = bytesColumnVector.start[idx];
                int len = bytesColumnVector.length[idx];
                byte[] tmp = new byte[len];

                boolean isUtf8FromLatin1 =
                    MySQLUnicodeUtils.utf8ToLatin1(bytesColumnVector.vector[idx], pos, pos + len, tmp);
                if (!isUtf8FromLatin1) {
                    // in columnar, decimals are stored already in latin1 encoding
                    System.arraycopy(bytesColumnVector.vector[idx], pos, tmp, 0, len);
                }
                DecimalStructure targetDec = new DecimalStructure();
                DecimalConverter.binToDecimal(tmp, targetDec, INPUT_TYPES.get(targetColumnId - 1).getPrecision(),
                    INPUT_TYPES.get(targetColumnId - 1).getScale());
                Assert.assertEquals(0,
                    FastDecimalUtils.compare(targetDec, block.getDecimal(row).getDecimalStructure()));
            }
        }
    }

    protected static void checkDecimal64(DecimalBlock block, VectorizedRowBatch batch, int targetColumnId) {
        Assert.assertTrue(block.isDecimal64());

        for (int row = 0; row < batch.size; row++) {
            for (int columnIndex = 0; columnIndex < batch.cols.length; columnIndex++) {
                if (targetColumnId != columnIndex + 1) {
                    continue;
                }

                ColumnVector vector = batch.cols[columnIndex];

                if (vector.isNull[row]) {
                    // check null
                    Assert.assertTrue(block.isNull(row));
                } else {
                    Assert.assertTrue(vector instanceof LongColumnVector);
                    Assert.assertEquals(block.getLong(row), ((LongColumnVector) vector).vector[row]);
                }
            }

        }
    }
}
