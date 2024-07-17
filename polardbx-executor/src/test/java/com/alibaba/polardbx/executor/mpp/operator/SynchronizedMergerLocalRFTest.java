package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.utils.XxhashUtils;
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItem;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemImpl;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFItemKey;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.mpp.planner.SimpleFragmentRFManager;
import com.alibaba.polardbx.executor.operator.ColumnarScanExec;
import com.alibaba.polardbx.executor.operator.SynchronizerRFMerger;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SynchronizedMergerLocalRFTest {
    public static final int THREAD_NUM = 4;
    private static final int TOTAL_CHUNK_SIZE = 1000 * 1000;
    private static final int CHUNK_LIMIT = 1000;

    private static final int MAX_VALUE_IN_RF = 3 * TOTAL_CHUNK_SIZE;
    private static final int TOTAL_PARTITION_COUNT = 4;
    private static final int TOTAL_WORKER_COUNT = 1;

    // initial value:
    // part = 0, value = 0
    // part = 1, value = 6
    // part = 2, value = 4
    // part = 3, value = 1
    private static final int[] INITIAL_PART_VALUE = {0, 6, 4, 1};

    private FragmentRFManager manager;
    private SynchronizerRFMerger merger;

    @Before
    public void setup() {
        // Initialize FragmentRFManager
        int totalPartitionCount = TOTAL_PARTITION_COUNT;
        int totalWorkerCount = TOTAL_WORKER_COUNT;
        double defaultFpp = 0.1d;
        int rowUpperBound = TOTAL_CHUNK_SIZE * 10;
        int rowLowerBound = 4096;
        double filterRatioThreshold = 0.25d;
        int rfSampleCount = 10;
        manager = new SimpleFragmentRFManager(
            totalPartitionCount, 4, defaultFpp,
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
        FragmentRFItem item = new FragmentRFItemImpl(
            manager,
            buildColumnName, probeColumnName,
            useXXHashInBuild, useXXHashInFilter,
            rfType
        );
        item.registerSource(Mockito.mock(ColumnarScanExec.class));
        manager.addItem(itemKey, item);

        // Initialize synchronizer merger
        final int blockChannel = 0;
        merger = new SynchronizerRFMerger(
            manager, item, THREAD_NUM, blockChannel);
    }

    @Test
    public void testAlignment() throws InterruptedException {

        List<ChunksIndex> chunksIndexList = new ArrayList<>();
        for (int part = 0; part < TOTAL_PARTITION_COUNT; part++) {

            ChunksIndex chunksIndex = new ChunksIndex();
            for (int i = 0; i < TOTAL_CHUNK_SIZE / CHUNK_LIMIT / TOTAL_PARTITION_COUNT; i++) {

                // fill array with initial part value.
                long[] array = new long[CHUNK_LIMIT];
                Arrays.fill(array, INITIAL_PART_VALUE[part]);

                for (int pos = 0; pos < CHUNK_LIMIT; pos++) {
                    int value = (i * CHUNK_LIMIT + pos) * 3;

                    // Just select the value that is an integer multiple of 3,
                    // and the part id is equal to this loop's part.
                    long hashVal = XxhashUtils.finalShuffle(value);
                    int partitionId = (int) ((hashVal & Long.MAX_VALUE) % TOTAL_PARTITION_COUNT);

                    if (partitionId == part) {
                        array[pos] = value;
                    }
                }

                LongBlock block = new LongBlock(0, CHUNK_LIMIT, null, array);
                Chunk chunk = new Chunk(CHUNK_LIMIT, block);
                chunksIndex.addChunk(chunk);
            }

            chunksIndexList.add(chunksIndex);
        }

        CountDownLatch latch = new CountDownLatch(THREAD_NUM);
        for (int threadId = 0; threadId < THREAD_NUM; threadId++) {
            TestTask task = new TestTask(chunksIndexList.get(threadId), latch);
            new Thread(task).start();
        }

        // wait for all thread tasks done.
        latch.await(2000, TimeUnit.MILLISECONDS);

        for (Map.Entry<FragmentRFItemKey, FragmentRFItem> entry : manager.getAllItems().entrySet()) {
            FragmentRFItem item = entry.getValue();

            // Check we have received the runtime filter.
            RFBloomFilter[] rfBloomFilters = item.getRF();
            Assert.assertTrue(rfBloomFilters.length == TOTAL_PARTITION_COUNT);
            for (int i = 0; i < TOTAL_PARTITION_COUNT; i++) {
                Assert.assertTrue(rfBloomFilters[i] != null);
            }

            // check all values in runtime filter.
            for (int part = 0; part < TOTAL_PARTITION_COUNT; part++) {

                // Get the runtime filter belonging to this partition.
                RFBloomFilter filter = rfBloomFilters[part];

                for (int i = 0; i < TOTAL_CHUNK_SIZE / CHUNK_LIMIT / TOTAL_PARTITION_COUNT; i++) {
                    for (int pos = 0; pos < CHUNK_LIMIT; pos++) {
                        int value = (i * CHUNK_LIMIT + pos) * 3;
                        // Just select the value that is an integer multiple of 3,
                        // and the part id is equal to this loop's part.
                        long hashVal = XxhashUtils.finalShuffle(value);
                        int partitionId = (int) ((hashVal & Long.MAX_VALUE) % TOTAL_PARTITION_COUNT);

                        if (partitionId == part) {
                            // Test the values in this partition.
                            Assert.assertTrue("value = " + value + ", part = " + partitionId,
                                filter.mightContainLong(value));
                        }
                    }
                }

            }
        }
    }

    private class TestTask implements Runnable {
        private final ChunksIndex builderKeyChunks;
        private final CountDownLatch latch;

        public TestTask(ChunksIndex builderKeyChunks, CountDownLatch latch) {
            this.builderKeyChunks = builderKeyChunks;
            this.latch = latch;
        }

        @Override
        public void run() {
            merger.addChunksForPartialRF(
                builderKeyChunks, 0, builderKeyChunks.getChunkCount(), false, 1);
            latch.countDown();
        }
    }
}
