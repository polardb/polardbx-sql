package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.utils.bloomfilter.BlockLongBloomFilter;
import com.alibaba.polardbx.common.utils.bloomfilter.RFBloomFilter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class RFReadWriteTest {
    private static final int BLOOM_FILTER_ELEMENT_COUNT = 4096;
    private static final int CHUNK_LIMIT = 1000;
    private static final int TOTAL_PARTITION_COUNT = 8;

    @Test
    public void testBroadcastRF() {
        // store 1 ~ 100
        LongBlockBuilder builder = new LongBlockBuilder(4);
        for (long i = 1; i <= 100; i++) {
            builder.writeLong(i);
        }
        Block block = builder.build();

        // global rf
        RFBloomFilter bloomFilter = new BlockLongBloomFilter(BLOOM_FILTER_ELEMENT_COUNT);

        // write into runtime filter
        block.addLongToBloomFilter(bloomFilter);

        // for rf checking
        boolean[] bitmap = new boolean[CHUNK_LIMIT];
        Arrays.fill(bitmap, false);

        // check 0 ~ 999
        builder = new LongBlockBuilder(4);
        for (long i = 0; i < 1000; i++) {
            builder.writeLong(i);
        }
        block = builder.build();

        int hitCount = block.mightContainsLong(bloomFilter, bitmap);
        Assert.assertTrue(hitCount == 100);
        for (int i = 0; i < 1000; i++) {
            if (i >= 1 && i <= 100) {
                Assert.assertTrue(bitmap[i]);
            } else {
                Assert.assertFalse(bitmap[i]);
            }
        }
    }

    @Test
    public void testBroadcastRFConjunctive() {
        // store 1 ~ 100
        LongBlockBuilder builder = new LongBlockBuilder(4);
        for (long i = 1; i <= 100; i++) {
            builder.writeLong(i);
        }
        Block block = builder.build();

        // global rf
        RFBloomFilter bloomFilter = new BlockLongBloomFilter(BLOOM_FILTER_ELEMENT_COUNT);

        // write into runtime filter
        block.addLongToBloomFilter(bloomFilter);

        // for rf checking
        boolean[] bitmap = new boolean[CHUNK_LIMIT];
        Arrays.fill(bitmap, true);
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 1) {
                bitmap[i] = false;
            }
        }

        // check 0 ~ 999
        builder = new LongBlockBuilder(4);
        for (long i = 0; i < 1000; i++) {
            builder.writeLong(i);
        }
        block = builder.build();

        int hitCount = block.mightContainsLong(bloomFilter, bitmap, true);
        Assert.assertTrue(hitCount == 50);
        for (int i = 0; i < 1000; i++) {
            if (i >= 1 && i <= 100 && i % 2 == 0) {
                Assert.assertTrue(bitmap[i]);
            } else {
                Assert.assertFalse(bitmap[i]);
            }
        }
    }

    @Test
    public void testLocalRF() {
        // store 1 ~ 100
        LongBlockBuilder builder = new LongBlockBuilder(4);
        for (long i = 1; i <= 100; i++) {
            builder.writeLong(i);
        }
        Block block = builder.build();

        // local rf
        RFBloomFilter[] rfBloomFilters = new RFBloomFilter[TOTAL_PARTITION_COUNT];
        for (int i = 0; i < TOTAL_PARTITION_COUNT; i++) {
            rfBloomFilters[i] = new BlockLongBloomFilter(BLOOM_FILTER_ELEMENT_COUNT);
        }

        // write into runtime filter
        block.addLongToBloomFilter(TOTAL_PARTITION_COUNT, rfBloomFilters);

        // for rf checking
        boolean[] bitmap = new boolean[CHUNK_LIMIT];
        Arrays.fill(bitmap, false);

        // check 0 ~ 999
        builder = new LongBlockBuilder(4);
        for (long i = 0; i < 1000; i++) {
            builder.writeLong(i);
        }
        block = builder.build();

        int hitCount = block.mightContainsLong(TOTAL_PARTITION_COUNT, rfBloomFilters,
            bitmap, false);
        Assert.assertTrue(hitCount == 100);
        for (int i = 0; i < 1000; i++) {
            if (i >= 1 && i <= 100) {
                Assert.assertTrue(bitmap[i]);
            } else {
                Assert.assertFalse(bitmap[i]);
            }
        }
    }

    @Test
    public void testLocalRFConjunctive() {
        // store 1 ~ 100
        LongBlockBuilder builder = new LongBlockBuilder(4);
        for (long i = 1; i <= 100; i++) {
            builder.writeLong(i);
        }
        Block block = builder.build();

        // local rf
        RFBloomFilter[] rfBloomFilters = new RFBloomFilter[TOTAL_PARTITION_COUNT];
        for (int i = 0; i < TOTAL_PARTITION_COUNT; i++) {
            rfBloomFilters[i] = new BlockLongBloomFilter(BLOOM_FILTER_ELEMENT_COUNT);
        }

        // write into runtime filter
        block.addLongToBloomFilter(TOTAL_PARTITION_COUNT, rfBloomFilters);

        // for rf checking
        boolean[] bitmap = new boolean[CHUNK_LIMIT];
        Arrays.fill(bitmap, true);
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 1) {
                bitmap[i] = false;
            }
        }

        // check 0 ~ 999
        builder = new LongBlockBuilder(4);
        for (long i = 0; i < 1000; i++) {
            builder.writeLong(i);
        }
        block = builder.build();

        int hitCount = block.mightContainsLong(TOTAL_PARTITION_COUNT, rfBloomFilters,
            bitmap, false, true);
        Assert.assertTrue(hitCount == 50);
        for (int i = 0; i < 1000; i++) {
            if (i >= 1 && i <= 100 && i % 2 == 0) {
                Assert.assertTrue(bitmap[i]);
            } else {
                Assert.assertFalse(bitmap[i]);
            }
        }
    }
}
