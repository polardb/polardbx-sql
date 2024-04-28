package com.alibaba.polardbx.common.utils.bloomfilter;

public interface RFBloomFilter {
    static BlockLongBloomFilter createBlockLongBloomFilter(int expectedEntries) {
        return new BlockLongBloomFilter(expectedEntries);
    }

    static ConcurrentIntBloomFilter createConcurrentIntBloomFilter(int expectedEntries, double fpp) {
        return ConcurrentIntBloomFilter.create(expectedEntries, fpp);
    }

    void putInt(int value);

    boolean mightContainInt(int value);

    void putLong(long value);

    boolean mightContainLong(long value);

    long sizeInBytes();

    void merge(RFBloomFilter other);
}
