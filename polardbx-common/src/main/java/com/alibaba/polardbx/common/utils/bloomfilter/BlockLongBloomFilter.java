package com.alibaba.polardbx.common.utils.bloomfilter;

/**
 * A special kind of blocked Bloom filter. It sets 2 to 4 (usually 4) bits in
 * two 64-bit words; 1 or 2 (usually 2) per word. It is faster than a regular
 * Bloom filter, but needs slightly more space / has a slightly worse false
 * positive rate.
 */
public class BlockLongBloomFilter implements RFBloomFilter {
    public static final int BITS_PER_KEY = 8;
    public static final long RANDOM_SEED = 2528582834704613611L;

    private final int buckets;
    private final long seed;
    private final long[] data;

    public BlockLongBloomFilter(int entryCount) {
        this(entryCount, BITS_PER_KEY);
    }

    public BlockLongBloomFilter(int entryCount, int bitsPerKey) {
        entryCount = Math.max(1, entryCount);
        this.seed = RANDOM_SEED;
        long bits = (long) entryCount * bitsPerKey;
        this.buckets = (int) bits / 64;
        data = new long[buckets + 16 + 1];
    }

    public long getBitCount() {
        return data.length * 64L;
    }

    @Override
    public void putLong(long key) {
        long hash = hash64(key, seed);
        int start = reduce((int) hash, buckets);
        hash = hash ^ Long.rotateLeft(hash, 32);
        long m1 = (1L << hash) | (1L << (hash >> 6));
        long m2 = (1L << (hash >> 12)) | (1L << (hash >> 18));

        data[start] |= m1;
        data[start + 1 + (int) (hash >>> 60)] |= m2;
    }

    @Override
    public boolean mightContainLong(long key) {
        long hash = hash64(key, seed);
        int start = reduce((int) hash, buckets);
        hash = hash ^ Long.rotateLeft(hash, 32);

        long a = data[start];
        long b = data[start + 1 + (int) (hash >>> 60)];
        long m1 = (1L << hash) | (1L << (hash >> 6));
        long m2 = (1L << (hash >> 12)) | (1L << (hash >> 18));
        return ((m1 & a) == m1) && ((m2 & b) == m2);
    }

    @Override
    public long sizeInBytes() {
        return data.length * Long.BYTES;
    }

    @Override
    public void merge(RFBloomFilter other) {
        if (other instanceof BlockLongBloomFilter && this.data.length == ((BlockLongBloomFilter) other).data.length
            && this.seed == ((BlockLongBloomFilter) other).seed
            && this.buckets == ((BlockLongBloomFilter) other).buckets) {

            long[] array = ((BlockLongBloomFilter) other).data;
            for (int i = 0; i < data.length; i++) {
                data[i] |= array[i];
            }

        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void putInt(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mightContainInt(int value) {
        throw new UnsupportedOperationException();
    }

    private static long hash64(long x, long seed) {
        x += seed;
        x = (x ^ x >>> 33) * -49064778989728563L;
        x = (x ^ x >>> 33) * -4265267296055464877L;
        x ^= x >>> 33;
        return x;
    }

    private static int reduce(int hash, int n) {
        return (int) (((long) hash & 4294967295L) * (long) n >>> 32);
    }
}
