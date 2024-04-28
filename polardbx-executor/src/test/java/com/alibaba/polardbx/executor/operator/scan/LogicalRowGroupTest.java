package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.columnar.LazyBlock;
import com.alibaba.polardbx.executor.operator.scan.impl.AsyncStripeLoader;
import com.alibaba.polardbx.executor.operator.scan.impl.RowGroupIteratorImpl;
import com.alibaba.polardbx.executor.operator.scan.impl.StaticStripePlanner;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileAccumulatorType;
import com.alibaba.polardbx.executor.operator.scan.metrics.ProfileUnit;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LogicalRowGroupTest extends ScanTestBase {

    public static final int PRIMARY_KEY_COL_ID = 3;

    // Test the enumeration of chunks in row-group iterator.
    @Test
    public void testChunkEnumerate() throws IOException {
        doTestChunkEnumerate(0);
    }

    @Test
    public void testChunkEnumerateMultiStripe() throws IOException {
        doTestChunkEnumerate(1);
    }

    @Test
    public void testSingleBlockLoader1() throws IOException {
        doTestSingle(1);
    }

    @Test
    public void testSingleBlockLoader2() throws IOException {
        doTestSingle(2);
    }

    private void doTestChunkEnumerate(int stripeId) throws IOException {
        // stripe info
        final StripeInformation stripeInformation = orcTail.getStripes().get(stripeId);
        final int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);

        // scan-range
        final boolean[] rgIncluded = new boolean[groupsInStripe];
        Arrays.fill(rgIncluded, false);
        rgIncluded[0] = true;
        rgIncluded[1] = true;
        rgIncluded[3] = true;
        final int startRowGroupId = 0;
        final int effectiveGroupCount = 3;

        // prepare block cache manager with actual cached block from orc file with given location.
        final int[] cachedGroupIds = {1};
        final int[] columnIds = {1, 2};
        final BlockCacheManager<Block> blockCacheManager = prepareCache(
            stripeId, columnIds, cachedGroupIds
        );

        final boolean[] columnIncluded = {true, true, true, false, false};
        final List<ColumnMeta> columnMetas = IntStream.range(1, columnIncluded.length)
            .filter(i -> columnIncluded[i])
            .mapToObj(i -> COLUMN_METAS.get(i - 1))
            .collect(Collectors.toList());

        final List<Integer> locInOrc = IntStream.range(1, columnIncluded.length)
            .filter(i -> columnIncluded[i])
            .mapToObj(i -> LOC_IN_ORC.get(i - 1))
            .collect(Collectors.toList());

        final ExecutionContext context = new ExecutionContext();
        final RuntimeMetrics metrics = RuntimeMetrics.create("StripeLoaderTest");
        // Add parent metrics node
        metrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_MEMORY,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);
        metrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_TIMER,
            null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);
        metrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_BYTES_RANGE,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

        metrics.addDerivedCounter(ColumnReader.COLUMN_READER_MEMORY,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);
        metrics.addDerivedCounter(ColumnReader.COLUMN_READER_TIMER,
            null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);

        metrics.addDerivedCounter(LogicalRowGroup.BLOCK_LOAD_TIMER,
            null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);
        metrics.addDerivedCounter(LogicalRowGroup.BLOCK_MEMORY_COUNTER,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

        OrcProto.ColumnEncoding[] encodings = StaticStripePlanner.buildEncodings(
            encryption, columnIncluded, preheatFileMeta.getStripeFooter(stripeId)
        );

        memoryAllocatorCtx = memoryAllocatorCtx;
        RowGroupIterator<Block, ColumnStatistics> rgIterator = new RowGroupIteratorImpl(
            metrics,

            // The range of this row-group iterator.
            stripeId,
            startRowGroupId,
            effectiveGroupCount,
            rgIncluded,

            new int[] {PRIMARY_KEY_COL_ID},

            // parameters for IO task.
            IO_EXECUTOR, FILESYSTEM, CONFIGURATION, FILE_PATH,

            // for compression
            compressionSize, compressionKind,
            preheatFileMeta,

            // for stripe-level parser
            stripeInformationMap.get(stripeId),
            startRowInStripeMap.get(stripeId),

            fileSchema, version, encryption, encodings, ignoreNonUtf8BloomFilter,
            maxBufferSize, maxDiskRangeChunkLimit, maxMergeDistance, DEFAULT_CHUNK_LIMIT, blockCacheManager,
            new OSSColumnTransformer(columnMetas, columnMetas, null, null, locInOrc),
            context, columnIncluded, indexStride,
            enableDecimal64, memoryAllocatorCtx);

        while (rgIterator.hasNext()) {
            rgIterator.next();
            LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup = rgIterator.current();

            // check logical row-group:
            System.out.println("group_id = " + logicalRowGroup.groupId());

            Chunk chunk;
            RowGroupReader<Chunk> rowGroupReader = logicalRowGroup.getReader();
            while ((chunk = rowGroupReader.nextBatch()) != null) {
                System.out.println("positionCount = " + chunk.getPositionCount());
                System.out.println("blockCount = " + chunk.getBlockCount());

                System.out.println(rowGroupReader.batchRange());

            }
        }
    }

    private void doTestSingle(int loadColumnIndex) throws IOException {
        // stripe info
        final int stripeId = 0;
        final StripeInformation stripeInformation = orcTail.getStripes().get(stripeId);
        final int groupsInStripe = (int) ((stripeInformation.getNumberOfRows() + indexStride - 1) / indexStride);

        // scan-range
        final boolean[] rgIncluded = new boolean[groupsInStripe];
        Arrays.fill(rgIncluded, false);
        rgIncluded[0] = true;
        rgIncluded[1] = true;
        rgIncluded[3] = true;
        final int startRowGroupId = 0;
        final int effectiveGroupCount = 3;

        // prepare block cache manager with actual cached block from orc file with given location.
        final int[] cachedGroupIds = {1};
        final int[] columnIds = {1, 2};
        final BlockCacheManager<Block> blockCacheManager = prepareCache(
            stripeId, columnIds, cachedGroupIds
        );

        final boolean[] columnIncluded = {true, true, true, false, false};
        final List<ColumnMeta> columnMetas = IntStream.range(1, columnIncluded.length)
            .filter(i -> columnIncluded[i])
            .mapToObj(i -> COLUMN_METAS.get(i - 1))
            .collect(Collectors.toList());

        final List<Integer> locInOrc = IntStream.range(1, columnIncluded.length)
            .filter(i -> columnIncluded[i])
            .mapToObj(i -> LOC_IN_ORC.get(i - 1))
            .collect(Collectors.toList());

//        final int loadColumnIndex = 1; // column index to load.
        final ExecutionContext context = new ExecutionContext();
        final RuntimeMetrics metrics = RuntimeMetrics.create("ScanWork$1");
        // Add parent metrics node
        metrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_MEMORY,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);
        metrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_TIMER,
            null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);
        metrics.addDerivedCounter(AsyncStripeLoader.ASYNC_STRIPE_LOADER_BYTES_RANGE,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

        metrics.addDerivedCounter(ColumnReader.COLUMN_READER_MEMORY,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);
        metrics.addDerivedCounter(ColumnReader.COLUMN_READER_TIMER,
            null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);

        metrics.addDerivedCounter(LogicalRowGroup.BLOCK_LOAD_TIMER,
            null, ProfileUnit.NANO_SECOND, ProfileAccumulatorType.SUM);
        metrics.addDerivedCounter(LogicalRowGroup.BLOCK_MEMORY_COUNTER,
            null, ProfileUnit.BYTES, ProfileAccumulatorType.SUM);

        OrcProto.ColumnEncoding[] encodings = StaticStripePlanner.buildEncodings(
            encryption, columnIncluded, preheatFileMeta.getStripeFooter(stripeId)
        );

        RowGroupIterator<Block, ColumnStatistics> rgIterator = new RowGroupIteratorImpl(
            metrics,

            // The range of this row-group iterator.
            stripeId,
            startRowGroupId,
            effectiveGroupCount,
            rgIncluded,

            new int[] {PRIMARY_KEY_COL_ID},

            // parameters for IO task.
            IO_EXECUTOR, FILESYSTEM, CONFIGURATION, FILE_PATH,

            // for compression
            compressionSize, compressionKind,
            preheatFileMeta,

            // for stripe-level parser
            stripeInformationMap.get(stripeId),
            startRowInStripeMap.get(stripeId),

            fileSchema, version, encryption, encodings, ignoreNonUtf8BloomFilter,
            maxBufferSize, maxDiskRangeChunkLimit, maxMergeDistance, DEFAULT_CHUNK_LIMIT, blockCacheManager,
            new OSSColumnTransformer(columnMetas, columnMetas, null, null, locInOrc),
            context, columnIncluded, indexStride,
            enableDecimal64, memoryAllocatorCtx);

        CacheReader<Block> cacheReader = rgIterator.getCacheReader(loadColumnIndex);
        if (cacheReader != null && !cacheReader.isInitialized()) {
            Map<Integer, SeekableIterator<Block>> caches = blockCacheManager
                .getCachedRowGroups(FILE_PATH, stripeId, loadColumnIndex, rgIncluded);
            cacheReader.initialize(caches);
        }

        ColumnReader columnReader = rgIterator.getColumnReader(loadColumnIndex);
        if (columnReader != null && cacheReader != null && !columnReader.isOpened()) {
            boolean[] cachedRowGroupBitmap = cacheReader.cachedRowGroupBitmap();
            boolean[] rowGroupIncluded = remove(rgIncluded, cachedRowGroupBitmap);

            printBitmap(rgIncluded);
            printBitmap(cachedRowGroupBitmap);
            printBitmap(rowGroupIncluded);

            columnReader.open(true, rowGroupIncluded);
        }

        final SortedMap<Integer, List<Chunk>> chunkBuffer = new TreeMap<>();
        while (rgIterator.hasNext()) {
            rgIterator.next();
            LogicalRowGroup<Block, ColumnStatistics> logicalRowGroup = rgIterator.current();
            final int rowGroupId = logicalRowGroup.groupId();

            Chunk chunk;
            RowGroupReader<Chunk> rowGroupReader = logicalRowGroup.getReader();
            while ((chunk = rowGroupReader.nextBatch()) != null) {

                LazyBlock lazyBlock = (LazyBlock) chunk.getBlock(loadColumnIndex - 1);
                // Proactively invoke loading, or we can load it during evaluation.
                lazyBlock.load();

//                for (int i = 0; i < chunk.getPositionCount(); i++) {
//                    System.out.println(lazyBlock.getObject(i));
//                }

                // Get selection array of this range [n * 1000, (n+1) * 1000] in row group,
                // and then evaluate the filter.
                int[] batchRange = rowGroupReader.batchRange();
                System.out.println("loaded batch range: " + batchRange);

                List<Chunk> chunksInGroup = chunkBuffer.computeIfAbsent(rowGroupId, any -> new ArrayList<>());
                chunksInGroup.add(chunk);
            }
        }

        System.out.println(metrics.reportAll());
    }

    private static boolean[] remove(boolean[] left, boolean[] right) {
        boolean[] result = new boolean[left.length];
        for (int i = 0; i < left.length; i++) {
            result[i] = left[i] &&
                (i >= right.length || (i < right.length && !right[i]));
        }
        return result;
    }

    private static void printBitmap(boolean[] bitmap) {
        StringBuilder builder = new StringBuilder();
        for (boolean b : bitmap) {
            builder.append(b ? 1 : 0);
        }
        System.out.println(builder);
    }
}
