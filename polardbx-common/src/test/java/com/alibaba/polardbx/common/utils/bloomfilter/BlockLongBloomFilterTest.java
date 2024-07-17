package com.alibaba.polardbx.common.utils.bloomfilter;

import org.junit.Assert;
import org.junit.Test;

public class BlockLongBloomFilterTest {
    @Test
    public void testMerge() {
        BlockLongBloomFilter b1 = new BlockLongBloomFilter(3_000_000);
        BlockLongBloomFilter b2 = new BlockLongBloomFilter(3_000_000);
        BlockLongBloomFilter global = new BlockLongBloomFilter(3_000_000);

        b1.putLong(5657485L);
        b1.putLong(892945435543543283L);

        b2.putLong(9542358545444L);
        b2.putLong(18992578498L);

        global.merge(b1);
        global.merge(b2);

        Assert.assertTrue(global.mightContainLong(5657485L));
        Assert.assertTrue(global.mightContainLong(892945435543543283L));
        Assert.assertTrue(global.mightContainLong(9542358545444L));
        Assert.assertTrue(global.mightContainLong(18992578498L));

        Assert.assertFalse(global.mightContainLong(5657485L + 1));
        Assert.assertFalse(global.mightContainLong(892945435543543283L + 1));
        Assert.assertFalse(global.mightContainLong(9542358545444L + 1));
        Assert.assertFalse(global.mightContainLong(18992578498L + 1));
    }
}
