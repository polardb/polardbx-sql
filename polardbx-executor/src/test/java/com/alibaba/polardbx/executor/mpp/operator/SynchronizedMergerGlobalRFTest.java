package com.alibaba.polardbx.executor.mpp.operator;

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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SynchronizedMergerGlobalRFTest {
    public static final int THREAD_NUM = 4;
    private static final int TOTAL_CHUNK_SIZE = 1000 * 1000;
    private static final int CHUNK_LIMIT = 1000;

    private static final int MAX_VALUE_IN_RF = 3 * TOTAL_CHUNK_SIZE;

    private FragmentRFManager manager;
    private SynchronizerRFMerger merger;

    @Before
    public void setup() {
        // Initialize FragmentRFManager
        int totalPartitionCount = -1;
        int totalWorkerCount = 4;
        double defaultFpp = 0.1d;
        int rowUpperBound = TOTAL_CHUNK_SIZE * 10;
        int rowLowerBound = 4096;
        double filterRatioThreshold = 0.25d;
        int rfSampleCount = 10;
        manager = new SimpleFragmentRFManager(
            totalPartitionCount, 1, defaultFpp,
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
        FragmentRFManager.RFType rfType = FragmentRFManager.RFType.BROADCAST;
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
    public void test() throws InterruptedException {
        ChunksIndex chunksIndex = new ChunksIndex();
        for (int i = 0; i < TOTAL_CHUNK_SIZE / CHUNK_LIMIT; i++) {

            // Build an array in which the data is an integer multiple of 3
            long[] array = new long[CHUNK_LIMIT];
            for (int pos = 0; pos < CHUNK_LIMIT; pos++) {
                array[pos] = (i * CHUNK_LIMIT + pos) * 3;
            }

            LongBlock block = new LongBlock(0, CHUNK_LIMIT, null, array);
            Chunk chunk = new Chunk(CHUNK_LIMIT, block);
            chunksIndex.addChunk(chunk);
        }

        CountDownLatch latch = new CountDownLatch(THREAD_NUM);
        for (int threadId = 0; threadId < THREAD_NUM; threadId++) {
            TestTask task = new TestTask(threadId, chunksIndex, latch);
            new Thread(task).start();
        }

        // wait for all thread tasks done.
        latch.await(2000, TimeUnit.MILLISECONDS);

        for (Map.Entry<FragmentRFItemKey, FragmentRFItem> entry : manager.getAllItems().entrySet()) {
            FragmentRFItem item = entry.getValue();

            // Check we have received the runtime filter.
            RFBloomFilter[] rfBloomFilters = item.getRF();
            Assert.assertTrue(rfBloomFilters.length == 1 && rfBloomFilters[0] != null);

            // check all values in runtime filter.
            RFBloomFilter filter = rfBloomFilters[0];
            for (int i = 0; i < MAX_VALUE_IN_RF; i++) {
                if (i % 3 == 0) {
                    Assert.assertTrue("value = " + i, filter.mightContainLong(i));
                }
            }
        }
    }

    private class TestTask implements Runnable {
        private final int threadId;
        private final ChunksIndex builderKeyChunks;
        private final CountDownLatch latch;

        public TestTask(int threadId, ChunksIndex builderKeyChunks, CountDownLatch latch) {
            this.threadId = threadId;
            this.builderKeyChunks = builderKeyChunks;
            this.latch = latch;
        }

        @Override
        public void run() {
            final int partitionSize = -Math.floorDiv(-builderKeyChunks.getChunkCount(), THREAD_NUM);
            final int startChunkId = partitionSize * threadId;

            if (startChunkId >= builderKeyChunks.getChunkCount()) {
                return; // skip invalid chunk ranges
            }

            final int endChunkId = Math.min(startChunkId + partitionSize, builderKeyChunks.getChunkCount());

            merger.addChunksForBroadcastRF(builderKeyChunks, startChunkId, endChunkId);
            latch.countDown();
        }
    }
}
