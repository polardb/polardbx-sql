package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.scan.impl.AbstractScanWork;
import com.alibaba.polardbx.executor.operator.scan.impl.MorselColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.impl.NonBlockedScanPreProcessor;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.rex.RexNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ColumnarReaderNeedCacheTest extends ScanTestBase {
    private final static ExecutorService SCAN_WORK_EXECUTOR = Executors.newFixedThreadPool(4);

    public static final int MORSEL_UNIT = 5;

    // need trace_id or other session-level parameters.
    private ExecutionContext context;

    // partial cache
    private BlockCacheManager<Block> blockCacheManager;

    // NOTE: The inputRefs in RexNode must be in consistent with inputRefForFilter.
    private RexNode predicate;
    private List<Integer> inputRefsForFilter;
    private LazyEvaluator<Chunk, BitSet> evaluator;
    private RoaringBitmap deletionBitmap;
    private SortedMap<Integer, boolean[]> matrix;
    private List<Integer> inputRefsForProject;

    @Before
    public void prepare() throws IOException {
        context = new ExecutionContext();
        context.setTraceId(TRACE_ID);

        // Cached ranges
        final int cachedStripeId = 0;
        final int[] columnIds = new int[] {1, 2};
        final int[] cachedGroupIds = new int[] {}; // empty, don't pre cache.
        blockCacheManager = prepareCache(
            cachedStripeId, columnIds, cachedGroupIds
        );

        // ($0 + 10000L) >= 0L
        predicate = null;

        // NOTE: The inputRefs in RexNode must be in consistent with inputRefForFilter.
        evaluator = null;

        inputRefsForFilter = ImmutableList.of();

        deletionBitmap = new RoaringBitmap();

        matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));
        inputRefsForProject = ImmutableList.of(0, 1, 2);

        context = new ExecutionContext();
        context.setTraceId(TRACE_ID);
    }

    @Test
    public void test1() throws Throwable {
        blockCacheManager.clear();

        // open ONLY_CACHE_PRIMARY_KEY_IN_BLOCK_CACHE
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.ONLY_CACHE_PRIMARY_KEY_IN_BLOCK_CACHE.getName(), true);
        context.setParamManager(new ParamManager(connectionMap));

        doTest();

        // Check if we only cache the primary key block.
        final int maxStripeId = 1;
        final int maxRowGroupId = 10;
        final int maxColumnId = 3;
        final int primaryKeyColumnId = 1;

        for (int stripeId = 0; stripeId < maxStripeId; stripeId++) {
            for (int columnId = 1; columnId <= maxColumnId; columnId++) {

                boolean[] rowGroupIncluded = new boolean[maxRowGroupId];
                Arrays.fill(rowGroupIncluded, true);

                Map<Integer, SeekableIterator<Block>> result =
                    blockCacheManager.getCachedRowGroups(FILE_PATH, stripeId, columnId, rowGroupIncluded);

                if (columnId != primaryKeyColumnId) {
                    Assert.assertTrue(result.isEmpty());
                } else {
                    Assert.assertTrue(!result.isEmpty());
                }

            }
        }

    }

    @Test
    public void test2() throws Throwable {
        blockCacheManager.clear();

        // close ONLY_CACHE_PRIMARY_KEY_IN_BLOCK_CACHE
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.ONLY_CACHE_PRIMARY_KEY_IN_BLOCK_CACHE.getName(), false);
        context.setParamManager(new ParamManager(connectionMap));

        doTest();

        // Check the blocks from all columns should be cached.
        final int maxStripeId = 1;
        final int maxRowGroupId = 10;
        final int maxColumnId = 3;

        for (int stripeId = 0; stripeId < maxStripeId; stripeId++) {
            for (int columnId = 1; columnId <= maxColumnId; columnId++) {

                boolean[] rowGroupIncluded = new boolean[maxRowGroupId];
                Arrays.fill(rowGroupIncluded, true);

                Map<Integer, SeekableIterator<Block>> result =
                    blockCacheManager.getCachedRowGroups(FILE_PATH, stripeId, columnId, rowGroupIncluded);

                Assert.assertTrue(!result.isEmpty());

            }
        }

    }

    public void doTest()
        throws Throwable {

        final int morselUnit = MORSEL_UNIT;

        NonBlockedScanPreProcessor preProcessor = new NonBlockedScanPreProcessor(
            preheatFileMeta, matrix, deletionBitmap
        );
        preProcessor.addFile(FILE_PATH);

        Set<Integer> refSet = new TreeSet<>();
        refSet.addAll(inputRefsForProject);
        refSet.addAll(inputRefsForFilter);
        List<ColumnMeta> columnMetas = refSet.stream().map(COLUMN_METAS::get).collect(Collectors.toList());
        List<Integer> locInOrc = refSet.stream().map(LOC_IN_ORC::get).collect(Collectors.toList());

        ColumnarSplit split = MorselColumnarSplit.newBuilder()
            .executionContext(context)
            .ioExecutor(IO_EXECUTOR)
            .fileSystem(FILESYSTEM, Engine.LOCAL_DISK)
            .configuration(CONFIGURATION)
            .sequenceId(SEQUENCE_ID)
            .file(FILE_PATH, FILE_ID)
            .columnTransformer(new OSSColumnTransformer(columnMetas, columnMetas, null, null, locInOrc))
            .inputRefs(inputRefsForFilter, inputRefsForProject)
            .cacheManager(blockCacheManager)
            .chunkLimit(DEFAULT_CHUNK_LIMIT)
            .morselUnit(morselUnit)
            .pushDown(evaluator)
            .prepare(preProcessor)
            .isColumnarMode(true)
            .tso(0L)
            .columnarManager(mockColumnarManager)
            .memoryAllocator(memoryAllocatorCtx)
            .build();

        ScanWork<ColumnarSplit, Chunk> scanWork;
        while ((scanWork = split.nextWork()) != null) {
            System.out.println(scanWork.getWorkId());

            MorselColumnarSplit.ScanRange scanRange =
                ((AbstractScanWork) scanWork).getScanRange();
            System.out.println("scan range: " + scanRange);

            // get status
            IOStatus<Chunk> ioStatus = scanWork.getIOStatus();
            scanWork.invoke(SCAN_WORK_EXECUTOR);

            // Get chunks according to state.
            boolean isCompleted = false;
            while (!isCompleted) {
                ScanState state = ioStatus.state();
                Chunk result;
                switch (state) {
                case READY:
                case BLOCKED: {
                    result = ioStatus.popResult();
                    if (result == null) {
                        ListenableFuture<?> listenableFuture = ioStatus.isBlocked();
                        listenableFuture.get();
                        result = ioStatus.popResult();
                    }
                    break;
                }
                case FINISHED:
                    while ((result = ioStatus.popResult()) != null) {

                    }
                    isCompleted = true;
                    break;
                case FAILED:
                    isCompleted = true;
                    ioStatus.throwIfFailed();
                    break;
                case CLOSED:
                    isCompleted = true;
                    break;
                }

            }

        }
    }
}