package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.utils.XxhashUtils;
import com.alibaba.polardbx.common.utils.bloomfilter.BlockLongBloomFilter;
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemImpl;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.mpp.planner.SimpleFragmentRFManager;
import com.alibaba.polardbx.executor.operator.scan.impl.RFLazyEvaluator;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Test for RFLazyEvaluator
public class RFLazyEvaluatorTest {
    private static final int TOTAL_ROW_COUNT = 1000 * 1000;
    private static final int CHUNK_LIMIT = 1000;
    private static final int PARTITION_COUNT = 4;

    @Test
    public void testBroadcastRF() {
        OperatorStatistics operatorStatistics = new OperatorStatistics();

        // Initialize FragmentRFManager
        int totalPartitionCount = PARTITION_COUNT;
        int totalWorkerCount = 4;
        double defaultFpp = 0.1d;
        int rowUpperBound = 100000;
        int rowLowerBound = 4096;
        double filterRatioThreshold = 0.25d;
        int rfSampleCount = 10;
        FragmentRFManager manager = new SimpleFragmentRFManager(
            totalPartitionCount, 1, defaultFpp,
            rowUpperBound, rowLowerBound,
            filterRatioThreshold, rfSampleCount
        );

        // create FragmentRFItemKey
        String buildColumnName = "build_col_1";
        String probeColumnName = "probe_col_1";
        int buildIndex = 0;
        int probeIndex = 0;
        FragmentRFItemKey itemKey = new FragmentRFItemKey(buildColumnName, probeColumnName, buildIndex, probeIndex);

        // Initialize a new FragmentRFItem.
        boolean useXXHashInBuild = true;
        boolean useXXHashInFilter = true;
        FragmentRFManager.RFType rfType = FragmentRFManager.RFType.BROADCAST;
        FragmentRFItem item = new FragmentRFItemImpl(
            manager,
            buildColumnName, probeColumnName,
            useXXHashInBuild, useXXHashInFilter,
            rfType
        );

        // Set parsed parameters about channel.
        int buildSideChannel = 0;
        int sourceFilterChannel = 0;
        int sourceRefInFile = 0;
        item.setBuildSideChannel(buildSideChannel);
        item.setSourceFilterChannel(sourceFilterChannel);
        item.setSourceRefInFile(sourceRefInFile);

        // assign runtime filters
        RFBloomFilter bloomFilter = new BlockLongBloomFilter(TOTAL_ROW_COUNT);
        for (int i = 0; i < TOTAL_ROW_COUNT; i++) {
            if (i % 3 == 0) {
                bloomFilter.putLong(i);
            }
        }
        RFBloomFilter[] bloomFilters = new RFBloomFilter[] {bloomFilter};
        item.assignRF(bloomFilters);
        manager.addItem(itemKey, item);

        Map<FragmentRFItemKey, RFBloomFilter[]> rfBloomFilterMap = new HashMap<>();
        rfBloomFilterMap.put(itemKey, bloomFilters);

        RFLazyEvaluator evaluator = new RFLazyEvaluator(manager, operatorStatistics, rfBloomFilterMap);

        for (int i = 0; i < TOTAL_ROW_COUNT / CHUNK_LIMIT; i++) {

            // 0 ~ 1000
            // 1000 ~ 2000
            // ...
            long[] values = new long[CHUNK_LIMIT];
            for (int j = 0; j < CHUNK_LIMIT; j++) {
                values[j] = i * CHUNK_LIMIT + j;
            }

            LongBlock block = new LongBlock(0, CHUNK_LIMIT, null, values);
            Chunk chunk = new Chunk(block);
            int startPos = i * CHUNK_LIMIT;
            int positionCount = CHUNK_LIMIT;

            // empty
            RoaringBitmap deletion = new RoaringBitmap();
            boolean[] bitmap = new boolean[CHUNK_LIMIT];

            // evaluate and check the filter ratio.
            int selectionCount = evaluator.eval(chunk, startPos, positionCount, deletion, bitmap);
            Assert.assertTrue("selectionCount = " + selectionCount + " lowerBound = "
                    + CHUNK_LIMIT / 3 + " upperBound = "
                    + CHUNK_LIMIT / 3.0d * (1 + 0.1d),
                selectionCount >= CHUNK_LIMIT / 3
                    && selectionCount <= CHUNK_LIMIT / 3.0d * (1 + 0.1d));
        }

    }

    @Test
    public void testLocalRF() {
        OperatorStatistics operatorStatistics = new OperatorStatistics();

        // Initialize FragmentRFManager
        int totalPartitionCount = PARTITION_COUNT;
        int totalWorkerCount = 4;
        double defaultFpp = 0.1d;
        int rowUpperBound = 100000;
        int rowLowerBound = 4096;
        double filterRatioThreshold = 0.25d;
        int rfSampleCount = 10;
        FragmentRFManager manager = new SimpleFragmentRFManager(
            totalPartitionCount, 1, defaultFpp,
            rowUpperBound, rowLowerBound,
            filterRatioThreshold, rfSampleCount
        );

        // For global RF
        // create FragmentRFItemKey
        FragmentRFItemKey itemKey = new FragmentRFItemKey(
            "build_col_1", "probe_col_1", 0, 0);

        // Initialize a new local FragmentRFItem.
        FragmentRFManager.RFType rfType = FragmentRFManager.RFType.LOCAL;
        boolean useXXHashInBuild = true;
        boolean useXXHashInFilter = true;
        FragmentRFItem item = new FragmentRFItemImpl(
            manager,
            "build_col_1", "probe_col_1",
            useXXHashInBuild, useXXHashInFilter,
            rfType
        );

        // Set parsed parameters about channel.
        int buildSideChannel = 0;
        int sourceFilterChannel = 0;
        int sourceRefInFile = 0;
        item.setBuildSideChannel(buildSideChannel);
        item.setSourceFilterChannel(sourceFilterChannel);
        item.setSourceRefInFile(sourceRefInFile);

        // assign runtime filters
        RFBloomFilter[] bloomFilters = new RFBloomFilter[PARTITION_COUNT];
        for (int i = 0; i < bloomFilters.length; i++) {
            bloomFilters[i] = new BlockLongBloomFilter(TOTAL_ROW_COUNT / PARTITION_COUNT);
        }

        for (int i = 0; i < TOTAL_ROW_COUNT; i++) {
            // Reserve values that are integer multiples of 3.
            if (i % 3 == 0) {
                // Route to partition
                long hashVal = XxhashUtils.finalShuffle(i);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % PARTITION_COUNT);

                bloomFilters[partition].putLong(i);
            }
        }
        item.assignRF(bloomFilters);

        manager.addItem(itemKey, item);

        Map<FragmentRFItemKey, RFBloomFilter[]> rfBloomFilterMap = new HashMap<>();
        rfBloomFilterMap.put(itemKey, bloomFilters);

        RFLazyEvaluator evaluator = new RFLazyEvaluator(manager, operatorStatistics, rfBloomFilterMap);

        Map<Integer, List<Chunk>> partitionChunks = new HashMap<>();
        Map<Integer, BlockBuilder> partitionBlockBuilders = new HashMap<>();
        for (int part = 0; part < PARTITION_COUNT; part++) {
            partitionBlockBuilders.put(part, new LongBlockBuilder(4));
        }

        for (int i = 0; i < TOTAL_ROW_COUNT; i++) {
            // Reserve values that are integer multiples of 3.
            if (i % 3 == 0) {
                // Route to partition
                long hashVal = XxhashUtils.finalShuffle(i);
                int partition = (int) ((hashVal & Long.MAX_VALUE) % PARTITION_COUNT);

                // write block builder
                BlockBuilder partitionBlockBuilder = partitionBlockBuilders.get(partition);
                partitionBlockBuilder.writeLong(i);

                // write chunks.
                if (partitionBlockBuilder.getPositionCount() >= CHUNK_LIMIT) {
                    List<Chunk> chunks = partitionChunks.computeIfAbsent(partition, any -> new ArrayList<>());
                    Block block = partitionBlockBuilder.build();
                    chunks.add(new Chunk(block));

                    partitionBlockBuilders.put(partition, new LongBlockBuilder(4));
                }
            }
        }

        // flush block builder
        for (int part = 0; part < PARTITION_COUNT; part++) {
            BlockBuilder partitionBlockBuilder = partitionBlockBuilders.get(part);
            if (partitionBlockBuilder.getPositionCount() > 0) {
                List<Chunk> chunks = partitionChunks.computeIfAbsent(part, any -> new ArrayList<>());
                Block block = partitionBlockBuilder.build();
                chunks.add(new Chunk(block));
            }
        }

        // For all chunks in all part, the hit rate is 100%
        for (int part = 0; part < PARTITION_COUNT; part++) {
            List<Chunk> chunks = partitionChunks.get(part);
            for (Chunk chunk : chunks) {
                int startPos = 0;
                final int positionCount = chunk.getPositionCount();
                // empty
                RoaringBitmap deletion = new RoaringBitmap();
                boolean[] bitmap = new boolean[positionCount];

                // evaluate and check the filter ratio.
                int selectionCount = evaluator.eval(chunk, startPos, positionCount, deletion, bitmap);
                Assert.assertTrue(selectionCount >= positionCount);
            }
        }

    }
}
