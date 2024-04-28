package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.utils.XxhashUtils;
import com.alibaba.polardbx.common.utils.bloomfilter.BlockLongBloomFilter;
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlockBuilder;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemImpl;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.mpp.planner.SimpleFragmentRFManager;
import com.alibaba.polardbx.executor.operator.ColumnarScanExec;
import com.alibaba.polardbx.executor.operator.SynchronizerRFMerger;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BfSizeOfPartitionTest {
    private static final int TOTAL_PARTITION_COUNT = 128;
    private static final int PARTITIONS_OF_NODE = 88;

    private static final int TOTAL_CHUNK_SIZE = 1000 * 1000;
    private static final int CHUNK_LIMIT = 1000;

    private FragmentRFManager manager;
    private FragmentRFItem item;
    private Map<Integer, Integer> valueOfEachPartition;

    @Before
    public void prepareRFManager() {
        // Initialize FragmentRFManager
        int totalPartitionCount = TOTAL_PARTITION_COUNT;
        double defaultFpp = 0.1d;
        int rowUpperBound = TOTAL_CHUNK_SIZE * 10;
        int rowLowerBound = 4096;
        double filterRatioThreshold = 0.25d;
        int rfSampleCount = 10;
        manager = new SimpleFragmentRFManager(
            totalPartitionCount, PARTITIONS_OF_NODE, defaultFpp,
            rowUpperBound, rowLowerBound,
            filterRatioThreshold, rfSampleCount
        );

        // create FragmentRFItemKey
        String buildColumnName = "build_col_1";
        String probeColumnName = "probe_col_1";
        int buildIndex = 0;
        int probeIndex = 0;
        FragmentRFItemKey itemKey = new FragmentRFItemKey(
            buildColumnName, probeColumnName, buildIndex, probeIndex);

        // Initialize a new FragmentRFItem.
        boolean useXXHashInBuild = true;
        boolean useXXHashInFilter = true;
        FragmentRFManager.RFType rfType = FragmentRFManager.RFType.LOCAL;
        item = new FragmentRFItemImpl(
            manager,
            buildColumnName, probeColumnName,
            useXXHashInBuild, useXXHashInFilter,
            rfType
        );
        item.registerSource(Mockito.mock(ColumnarScanExec.class));
        manager.addItem(itemKey, item);
    }

    @Before
    public void prepareValueOfEachPartition() {
        valueOfEachPartition = new HashMap<>();
        for (int intVal = 0; intVal < 1500; intVal++) {

            // enumerate values to hit all partitions.
            long hashVal = XxhashUtils.finalShuffle(intVal);
            int partition = (int) ((hashVal & Long.MAX_VALUE) % TOTAL_PARTITION_COUNT);

            // directly replace old values.
            valueOfEachPartition.put(partition, intVal);
        }
    }

    @Test
    public void testParallelismLessThanPartitionCount() {
        final int buildSideParallelism = 20;
        final int localPartitionCount = PARTITIONS_OF_NODE;
        final int synchronizerNum = Math.min(buildSideParallelism, localPartitionCount);

        // Get the count of partition for each bucket.
        int[] partitionsOfEachBucket =
            ExecUtils.partitionsOfEachBucket(localPartitionCount, buildSideParallelism);
        for (int i = 0; i < partitionsOfEachBucket.length; i++) {
            System.out.println("bucketId = " + i + " parts = " + partitionsOfEachBucket[i]);
        }

        // Initialize synchronizer merger
        final int blockChannel = 0;
        SynchronizerRFMerger merger = new SynchronizerRFMerger(
            manager, item, buildSideParallelism, blockChannel);

        // Store the ChunksIndex object for each synchronizer instance.
        Map<Integer, ChunksIndex> chunksIndexMap = new HashMap<>();

        // we assume that the partitions p0 ~ p87 were allocated in this node.
        for (int partIndex = 0; partIndex < PARTITIONS_OF_NODE; partIndex++) {
            int synchronizerIndex = partIndex % synchronizerNum;

            ChunksIndex chunksIndex = chunksIndexMap.computeIfAbsent(synchronizerIndex, any -> new ChunksIndex());

            // put intVal of specific partition into chunksIndex of this synchronizer.
            int intVal = valueOfEachPartition.get(partIndex);
            for (int loop = 0; loop < 9; loop++) {
                IntegerBlockBuilder blockBuilder = new IntegerBlockBuilder(CHUNK_LIMIT);
                for (int pos = 0; pos < CHUNK_LIMIT; pos++) {
                    blockBuilder.writeInt(intVal);
                }
                Block block = blockBuilder.build();
                Chunk chunk = new Chunk(block);
                chunksIndex.addChunk(chunk);
            }
        }

        Assert.assertTrue(merger.getBfParallelismCounter().get() == buildSideParallelism);

        // for each hash table (synchronizer), put chunk indexes to build runtime filter.
        for (int hashTableIndex = 0; hashTableIndex < synchronizerNum; hashTableIndex++) {
            ChunksIndex chunksIndex = chunksIndexMap.get(hashTableIndex);

            int partitionsInSynchronizer = partitionsOfEachBucket[hashTableIndex];

            merger.addChunksForPartialRF(
                chunksIndex, 0, chunksIndex.getChunkCount(),
                false, partitionsInSynchronizer
            );
        }

        // all bf in all parallelism have been done.
        Assert.assertTrue(merger.getBfParallelismCounter().get() == 0);

        // check RF Item
        RFBloomFilter[] rfBloomFilters = item.getRF();
        for (int partIndex = 0; partIndex < rfBloomFilters.length; partIndex++) {

            // runtime filter in partitions p0 ~ p87 must be not null.
            Assert.assertTrue((partIndex >= PARTITIONS_OF_NODE && rfBloomFilters[partIndex] == null)
                || (partIndex < PARTITIONS_OF_NODE && rfBloomFilters[partIndex] != null));

            if (partIndex >= PARTITIONS_OF_NODE) {
                continue;
            }

            int bucketIndex = partIndex % synchronizerNum;
            int partitionsInSynchronizer = partitionsOfEachBucket[bucketIndex];
            int buildSideRows = chunksIndexMap.get(bucketIndex).getPositionCount();
            long expectedBfSizeOfPartition = (long) Math.ceil(buildSideRows * 1.0d / partitionsInSynchronizer);

            // algorithm of bucket size of block bloom filter.
            int bucket = (int) (Math.max(1, expectedBfSizeOfPartition) * 8) / 64 + 16 + 1;
            long expectedBitCount = bucket * 64;

            RFBloomFilter rf = rfBloomFilters[partIndex];
            BlockLongBloomFilter blockLongBloomFilter = (BlockLongBloomFilter) rf;

            Assert.assertEquals(expectedBitCount, blockLongBloomFilter.getBitCount());
        }

    }

    @Test
    public void testParallelismGreaterThanPartitionCount() {
        final int buildSideParallelism = 196;
        final int localPartitionCount = PARTITIONS_OF_NODE;
        final int synchronizerNum = Math.min(buildSideParallelism, localPartitionCount);

        // Get the count of partition for each bucket.
        int[] partitionsOfEachBucket =
            ExecUtils.partitionsOfEachBucket(localPartitionCount, buildSideParallelism);
        for (int i = 0; i < partitionsOfEachBucket.length; i++) {
            System.out.println("bucketId = " + i + " parts = " + partitionsOfEachBucket[i]);
        }

        // Initialize synchronizer merger
        final int blockChannel = 0;
        SynchronizerRFMerger merger = new SynchronizerRFMerger(
            manager, item, buildSideParallelism, blockChannel);

        // Store the ChunksIndex object for each synchronizer instance.
        Map<Integer, ChunksIndex> chunksIndexMap = new HashMap<>();

        // we assume that the partitions p0 ~ p87 were allocated in this node.
        for (int partIndex = 0; partIndex < PARTITIONS_OF_NODE; partIndex++) {
            int synchronizerIndex = partIndex % synchronizerNum;

            ChunksIndex chunksIndex = chunksIndexMap.computeIfAbsent(synchronizerIndex, any -> new ChunksIndex());

            // put intVal of specific partition into chunksIndex of this synchronizer.
            int intVal = valueOfEachPartition.get(partIndex);
            for (int loop = 0; loop < 9; loop++) {
                IntegerBlockBuilder blockBuilder = new IntegerBlockBuilder(CHUNK_LIMIT);
                for (int pos = 0; pos < CHUNK_LIMIT; pos++) {
                    blockBuilder.writeInt(intVal);
                }
                Block block = blockBuilder.build();
                Chunk chunk = new Chunk(block);
                chunksIndex.addChunk(chunk);
            }
        }

        List<Integer> bucketIdOfEachParallelism =
            ExecUtils.assignPartitionToExecutor(localPartitionCount, buildSideParallelism);
        Assert.assertEquals(synchronizerNum, bucketIdOfEachParallelism.stream().distinct().count());

        // Calculate parallelism of each bucket.
        int[] parallelismOfEachBucket = new int[synchronizerNum];
        for (int parallelism = 0; parallelism < buildSideParallelism; parallelism++) {
            parallelismOfEachBucket[bucketIdOfEachParallelism.get(parallelism)]++;
        }

        Assert.assertTrue(merger.getBfParallelismCounter().get() == buildSideParallelism);

        // for each hash table (synchronizer), put chunk indexes to build runtime filter.
        for (int hashTableIndex = 0; hashTableIndex < synchronizerNum; hashTableIndex++) {
            ChunksIndex chunksIndex = chunksIndexMap.get(hashTableIndex);

            int partitionsInSynchronizer = partitionsOfEachBucket[hashTableIndex];

            int parallelismInSynchronizer = parallelismOfEachBucket[hashTableIndex];

            for (int partition = 0; partition < parallelismInSynchronizer; partition++) {
                final int partitionSize = -Math.floorDiv(-chunksIndex.getChunkCount(), parallelismInSynchronizer);
                final int startChunkId = partitionSize * partition;
                final int endChunkId = Math.min(startChunkId + partitionSize, chunksIndex.getChunkCount());
                merger.addChunksForPartialRF(
                    chunksIndex, startChunkId, endChunkId,
                    false, partitionsInSynchronizer
                );
            }

        }

        // all bf in all parallelism have been done.
        Assert.assertTrue(merger.getBfParallelismCounter().get() == 0);

        // check RF Item
        RFBloomFilter[] rfBloomFilters = item.getRF();
        for (int partIndex = 0; partIndex < rfBloomFilters.length; partIndex++) {

            // runtime filter in partitions p0 ~ p87 must be not null.
            Assert.assertTrue((partIndex >= PARTITIONS_OF_NODE && rfBloomFilters[partIndex] == null)
                || (partIndex < PARTITIONS_OF_NODE && rfBloomFilters[partIndex] != null));

            if (partIndex >= PARTITIONS_OF_NODE) {
                continue;
            }

            int bucketIndex = partIndex % synchronizerNum;
            int partitionsInSynchronizer = partitionsOfEachBucket[bucketIndex];
            int buildSideRows = chunksIndexMap.get(bucketIndex).getPositionCount();
            long expectedBfSizeOfPartition = (long) Math.ceil(buildSideRows * 1.0d / partitionsInSynchronizer);

            // algorithm of bucket size of block bloom filter.
            int bucket = (int) (Math.max(1, expectedBfSizeOfPartition) * 8) / 64 + 16 + 1;
            long expectedBitCount = bucket * 64;

            RFBloomFilter rf = rfBloomFilters[partIndex];
            BlockLongBloomFilter blockLongBloomFilter = (BlockLongBloomFilter) rf;

            Assert.assertEquals(expectedBitCount, blockLongBloomFilter.getBitCount());
        }

    }

}
