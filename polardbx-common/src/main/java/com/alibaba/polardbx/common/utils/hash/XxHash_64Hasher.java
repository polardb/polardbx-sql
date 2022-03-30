package com.alibaba.polardbx.common.utils.hash;

import com.alibaba.polardbx.common.utils.memory.SizeOf;

import static com.alibaba.polardbx.common.utils.hash.ByteUtil.getBlock32Uncheck;
import static com.alibaba.polardbx.common.utils.hash.ByteUtil.getBlock64Uncheck;
import static java.lang.Long.rotateLeft;

/**
 * 64bit hash 仅使用result1存放结果
 */
public final class XxHash_64Hasher extends BufferedHasher implements IBlockHasher {

    private static final long PRIME64_1 = -7046029288634856825L;
    private static final long PRIME64_2 = -4417276706812531889L;
    private static final long PRIME64_3 = 1609587929392839161L;
    private static final long PRIME64_4 = -8796714831421723037L;
    private static final long PRIME64_5 = 2870177450012600261L;

    private static final int BLOCK_SIZE = 32;
    private static final int BUFFER_SIZE = BLOCK_SIZE + 7;

    private final long v1;
    private final long v2;
    private final long v3;
    private final long v4;

    private long h1;
    private long h2;
    private long h3;
    private long h4;

    public XxHash_64Hasher(int seed) {
        super(BUFFER_SIZE, BLOCK_SIZE, seed);
        this.v1 = seed + PRIME64_1 + PRIME64_2;
        this.v2 = seed + PRIME64_2;
        this.v3 = seed;
        this.v4 = seed - PRIME64_1;
        reset();
    }

    @Override
    public void reset() {
        pos = 0;
        processedLength = 0;
        h1 = v1;
        h2 = v2;
        h3 = v3;
        h4 = v4;
    }

    @Override
    public HashResult128 hashLong(long l) {
        long hash = seed + PRIME64_5 + SizeOf.SIZE_OF_LONG;
        hash = mixForTail(hash, l);
        finalHashThenStore(hash);
        return this.result;
    }

    @Override
    public HashResult128 hashBytes(byte[] bytes) {
        int length = bytes.length;
        long hash = length;
        if (length >= BLOCK_SIZE) {
            int nblocks = length / BLOCK_SIZE;
            hash += hashBodyByBlock(bytes, 0, nblocks);
        } else {
            hash += (seed + PRIME64_5);
        }

        int tailStart = length & 0xFFFFFFE0;
        hash = processBlockTail(hash, bytes, tailStart, length);
        finalHashThenStore(hash);
        return this.result;
    }

    @Override
    public void innerHash() {
        long hash;
        if (processedLength > 0) {
            hash = getMixedHash(h1, h2, h3, h4);
        } else {
            hash = seed + PRIME64_5;
        }

        hash += processedLength + pos;
        hash = processBlockTail(hash, buffer, 0, pos);

        finalHashThenStore(hash);
    }

    @Override
    protected void processBlockBody(final byte[] data, final int start, final int nBlocks) {
        for (int i = 0; i < nBlocks; i++) {
            h1 = mix(h1, getBlock64Uncheck(data, start + (i * 4) * 8));
            h2 = mix(h2, getBlock64Uncheck(data, start + (i * 4 + 1) * 8));
            h3 = mix(h3, getBlock64Uncheck(data, start + (i * 4 + 2) * 8));
            h4 = mix(h4, getBlock64Uncheck(data, start + (i * 4 + 3) * 8));
        }
    }

    private long processBlockTail(long hash, final byte[] data, int tailStart, int length) {
        while (length - tailStart >= 8) {
            hash = mixForTail(hash, getBlock64Uncheck(data, tailStart));
            tailStart += 8;
        }

        if (length - tailStart >= 4) {
            hash = processTail(hash, getBlock32Uncheck(data, tailStart));
            tailStart += 4;
        }

        while (tailStart < length) {
            hash = processTail(hash, data[tailStart]);
            tailStart++;
        }

        return hash;
    }

    private void finalHashThenStore(long hash) {
        hash ^= hash >>> 33;
        hash *= PRIME64_2;
        hash ^= hash >>> 29;
        hash *= PRIME64_3;
        hash ^= hash >>> 32;

        this.result.result1 = hash;
    }

    /**
     * 不修改成员状态
     */
    private long hashBodyByBlock(byte[] data, int start, int nblocks) {
        long k1 = v1, k2 = v2, k3 = v3, k4 = v4;
        for (int i = 0; i < nblocks; i++) {
            k1 = mix(k1, getBlock64Uncheck(data, start + (i * 4) * 8));
            k2 = mix(k2, getBlock64Uncheck(data, start + (i * 4 + 1) * 8));
            k3 = mix(k3, getBlock64Uncheck(data, start + (i * 4 + 2) * 8));
            k4 = mix(k4, getBlock64Uncheck(data, start + (i * 4 + 3) * 8));
        }

        return getMixedHash(k1, k2, k3, k4);
    }

    private static long getMixedHash(long v1, long v2, long v3, long v4) {
        long hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) +
            rotateLeft(v3, 12) + rotateLeft(v4, 18);

        hash = hmix(hash, v1);
        hash = hmix(hash, v2);
        hash = hmix(hash, v3);
        hash = hmix(hash, v4);

        return hash;
    }

    private static long mix(long current, long value) {
        return rotateLeft(current + value * PRIME64_2, 31) * PRIME64_1;
    }

    private static long hmix(long hash, long value) {
        long temp = hash ^ mix(0, value);
        return temp * PRIME64_1 + PRIME64_4;
    }

    private static long mixForTail(long hash, long value) {
        long temp = hash ^ mix(0, value);
        return rotateLeft(temp, 27) * PRIME64_1 + PRIME64_4;
    }

    private static long processTail(long hash, int value) {
        long unsigned = value & 0xFFFF_FFFFL;
        long temp = hash ^ (unsigned * PRIME64_1);
        return rotateLeft(temp, 23) * PRIME64_2 + PRIME64_3;
    }

    private static long processTail(long hash, byte value) {
        int unsigned = value & 0xFF;
        long temp = hash ^ (unsigned * PRIME64_5);
        return rotateLeft(temp, 11) * PRIME64_1;
    }
}
