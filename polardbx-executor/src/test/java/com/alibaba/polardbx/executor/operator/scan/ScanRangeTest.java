package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.scan.impl.AbstractScanWork;
import com.alibaba.polardbx.executor.operator.scan.impl.MorselColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.impl.NonBlockedScanPreProcessor;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.rex.RexNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openjdk.jol.info.GraphLayout;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ScanRangeTest extends ScanTestBase {
    private final static ExecutorService SCAN_WORK_EXECUTOR = Executors.newFixedThreadPool(4);

    public static final int MORSEL_UNIT = 1024;
    private static final long ESTIMATED_SIZE_IN_BYTES_BEFORE_CLOSE = 8 * 1024 * 1024; // 8MB

    // need trace_id or other session-level parameters.
    private ExecutionContext context;

    // partial cache
    private BlockCacheManager<Block> blockCacheManager;

    // NOTE: The inputRefs in RexNode must be in consistent with inputRefForFilter.
    private RexNode predicate;
    private List<Integer> inputRefsForFilter;
    private LazyEvaluator<Chunk, BitSet> evaluator;
    private RoaringBitmap deletionBitmap;
    private TreeMap<Integer, boolean[]> matrix;

    @Before
    public void prepare() throws IOException {
        context = new ExecutionContext();
        context.setTraceId(TRACE_ID);

        // Cached ranges
        final int cachedStripeId = 0;
        final int[] columnIds = new int[] {1, 2};
        final int[] cachedGroupIds = new int[] {0, 2};
        blockCacheManager = prepareCache(
            cachedStripeId, columnIds, cachedGroupIds
        );

        // ($0 + 10000L) >= 0L
        predicate = null;

        // NOTE: The inputRefs in RexNode must be in consistent with inputRefForFilter.
        evaluator = null;
        inputRefsForFilter = ImmutableList.of();
        deletionBitmap = new RoaringBitmap();
    }

    @Test
    public void test1() throws Throwable {
        matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));
        matrix.put(1, fromRowGroupIds(1, new int[] {2, 4, 7, 11, 19, 20}));
        matrix.put(2, fromRowGroupIds(2, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));
        matrix.put(3, fromRowGroupIds(3, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2, 3)
        );
    }

    @Test
    public void test2() throws Throwable {
        matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));
        matrix.put(1, fromRowGroupIds(1, new int[] {}));
        matrix.put(2, fromRowGroupIds(2, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));
        matrix.put(3, fromRowGroupIds(3, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2, 3)
        );
    }

    @Test
    public void test3() throws Throwable {
        matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));
        matrix.put(1, fromRowGroupIds(1, new int[] {2, 4, 7, 11, 19, 20}));
        matrix.put(2, fromRowGroupIds(2, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));
        matrix.put(3, fromRowGroupIds(3, new int[] {}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2, 3)
        );
    }

    @Test
    public void test4() throws Throwable {
        matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {}));
        matrix.put(1, fromRowGroupIds(1, new int[] {2, 4, 7, 11, 19, 20}));
        matrix.put(2, fromRowGroupIds(2, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));
        matrix.put(3, fromRowGroupIds(3, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2, 3)
        );
    }

    @Test
    public void test5() throws Throwable {
        matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {}));
        matrix.put(1, fromRowGroupIds(1, new int[] {}));
        matrix.put(2, fromRowGroupIds(2, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));
        matrix.put(3, fromRowGroupIds(3, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2, 3)
        );
    }

    @Test
    public void test6() throws Throwable {
        matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {}));
        matrix.put(1, fromRowGroupIds(1, new int[] {}));
        matrix.put(2, fromRowGroupIds(2, new int[] {}));
        matrix.put(3, fromRowGroupIds(3, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2, 3)
        );
    }

    @Test
    public void test7() throws Throwable {
        matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {}));
        matrix.put(1, fromRowGroupIds(1, new int[] {2, 4, 7, 11, 19, 20}));
        matrix.put(2, fromRowGroupIds(2, new int[] {}));
        matrix.put(3, fromRowGroupIds(3, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2, 3)
        );
    }

    @Test
    public void test8() throws Throwable {
        matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {}));
        matrix.put(1, fromRowGroupIds(1, new int[] {}));
        matrix.put(2, fromRowGroupIds(2, new int[] {}));
        matrix.put(3, fromRowGroupIds(3, new int[] {2, 4, 7, 11, 19, 20}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2, 3)
        );
    }

    public void doTest(
        SortedMap<Integer, boolean[]> rowGroupMatrix,
        List<Integer> inputRefsForProject
    )
        throws Throwable {

        long sizeInBytesBeforeClose = ESTIMATED_SIZE_IN_BYTES_BEFORE_CLOSE;

        Map<String, ScanWork<ColumnarSplit, Chunk>> finishedWorks = new TreeMap<>();

        context = new ExecutionContext();
        context.setTraceId(TRACE_ID);

        final int morselUnit = MORSEL_UNIT;

        NonBlockedScanPreProcessor preProcessor = new NonBlockedScanPreProcessor(
            preheatFileMeta, rowGroupMatrix, deletionBitmap
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
            .columnarManager(mockColumnarManager)
            .memoryAllocator(memoryAllocatorCtx)
            .build();

        Iterator<Integer> iterator = new CustomIterator(matrix);

        ScanWork<ColumnarSplit, Chunk> scanWork;
        while ((scanWork = split.nextWork()) != null) {
            System.out.println(scanWork.getWorkId());

            MorselColumnarSplit.ScanRange scanRange =
                ((AbstractScanWork) scanWork).getScanRange();
            System.out.println("scan range: " + scanRange);
            Assert.assertTrue(iterator.hasNext());
            int expectedStripeId = iterator.next();
            Assert.assertEquals(expectedStripeId, scanRange.getStripeId());

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

                    // stripe = ...
                    // rgMatrix = ...
                    // proj_column = ...
                    // filter = ...
                    break;
                }
                case FINISHED:

                    while ((result = ioStatus.popResult()) != null) {

                    }
                    isCompleted = true;

                    finishedWorks.put(scanWork.getWorkId(), scanWork);
                    long sizeInBytesAfterClose = GraphLayout.parseInstance(scanWork).totalSize();
                    System.out.println("object size = " + GraphLayout.parseInstance(scanWork).totalSize());
                    Assert.assertTrue(sizeInBytesAfterClose * 100 < sizeInBytesBeforeClose);

                    break;
                case FAILED:
                    isCompleted = true;
                    ioStatus.throwIfFailed();

                    break;
                case CLOSED:
                    isCompleted = true;

                    finishedWorks.put(scanWork.getWorkId(), scanWork);
                    break;
                }

            }
            Preconditions.checkArgument(((AbstractScanWork) scanWork).checkIfAllReadersClosed());

        }

        Assert.assertTrue(!iterator.hasNext());

    }

    public class CustomIterator implements Iterator<Integer> {
        private Iterator<Map.Entry<Integer, boolean[]>> iterator;
        private Integer nextKey;

        public CustomIterator(TreeMap<Integer, boolean[]> treeMap) {
            this.iterator = treeMap.entrySet().iterator();
            findNext();
        }

        private void findNext() {
            nextKey = null;
            while (iterator.hasNext()) {
                Map.Entry<Integer, boolean[]> entry = iterator.next();
                if (!isAllFalse(entry.getValue())) {
                    nextKey = entry.getKey();
                    break;
                }
            }
        }

        private boolean isAllFalse(boolean[] array) {
            for (boolean b : array) {
                if (b) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean hasNext() {
            return nextKey != null;
        }

        @Override
        public Integer next() {
            if (nextKey == null) {
                throw new java.util.NoSuchElementException();
            }
            Integer currentKey = nextKey;
            findNext(); // set up the next key
            return currentKey;
        }
    }
}

