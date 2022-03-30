package com.alibaba.polardbx.common.utils.hash;

import javax.annotation.concurrent.NotThreadSafe;

import static com.alibaba.polardbx.common.utils.hash.ByteUtil.getBlock64Uncheck;

/**
 * Murmur3哈希函数
 * 实现与DN对齐
 * 支持流式运算
 * 不支持增量式获取哈希结果 即每次得到最终结果会重置状态
 * TODO 后续用Unsafe实现
 */
@NotThreadSafe
public final class Murmur3_128Hasher extends BufferedHasher implements IBlockHasher {

    private static final int BLOCK_SIZE = 16;
    private static final int BUFFER_SIZE = 16 + 7;

    private static final long c1 = 0x87c37b91114253d5L;
    private static final long c2 = 0x4cf5ad432745937fL;

    private long h1;
    private long h2;

    public Murmur3_128Hasher(int seed) {
        super(BUFFER_SIZE, BLOCK_SIZE, seed);
    }

    public void reset() {
        h1 = h2 = seed;
        pos = 0;
        processedLength = 0;
    }

    @Override
    public void innerHash() {
        processIfFull();
        if (pos > 0) {
            processedLength += pos;
            processBlockTail(buffer, 0, processedLength);
        }
        finalHashThenStore(processedLength);
    }

    /**
     * 以16字节为一个block进行处理
     * 后期data可以用unsafe实现作为唯一入口
     *
     * @param start 作为预留的offset使用
     * @param len 需要处理的长度
     */
    private void processByBlock64(final byte[] data, final int start, final int len) {
        reset();

        // 按块处理
        final int nBlocks = len / BLOCK_SIZE;
        processBlockBody(data, start, nBlocks);

        // 处理尾部
        final int tailOffset = start + nBlocks * BLOCK_SIZE;
        processBlockTail(data, tailOffset, len);

        // hash并存储结果
        finalHashThenStore(len);
    }

    private void processBlockTail(byte[] data, final int tailOffset, final int totalLen) {
        long k1 = 0;
        long k2 = 0;
        switch (totalLen & 15) {    // 尾部数据长度, 此处基于BLOCK_SIZE为16取余
        case 15:
            k2 ^= (long) (data[tailOffset + 14] & 0xFF) << 48;
        case 14:
            k2 ^= (long) (data[tailOffset + 13] & 0xFF) << 40;
        case 13:
            k2 ^= (long) (data[tailOffset + 12] & 0xFF) << 32;
        case 12:
            k2 ^= (long) (data[tailOffset + 11] & 0xFF) << 24;
        case 11:
            k2 ^= (long) (data[tailOffset + 10] & 0xFF) << 16;
        case 10:
            k2 ^= (long) (data[tailOffset + 9] & 0xFF) << 8;
        case 9:
            k2 ^= (long) data[tailOffset + 8] & 0xFF;
            k2 *= c2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= c1;
            h2 ^= k2;

        case 8:
            k1 ^= (long) (data[tailOffset + 7] & 0xFF) << 56;
        case 7:
            k1 ^= (long) (data[tailOffset + 6] & 0xFF) << 48;
        case 6:
            k1 ^= (long) (data[tailOffset + 5] & 0xFF) << 40;
        case 5:
            k1 ^= (long) (data[tailOffset + 4] & 0xFF) << 32;
        case 4:
            k1 ^= (long) (data[tailOffset + 3] & 0xFF) << 24;
        case 3:
            k1 ^= (long) (data[tailOffset + 2] & 0xFF) << 16;
        case 2:
            k1 ^= (long) (data[tailOffset + 1] & 0xFF) << 8;
        case 1:
            k1 ^= (long) data[tailOffset] & 0xFF;

            k1 *= c1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= c2;
            h1 ^= k1;
            break;
        default:
            throw new AssertionError("Should not get here while process BlockTail in murmur3.");
        }
    }

    @Override
    protected void processBlockBody(final byte[] data, final int start, final int nBlocks) {
        for (int i = 0; i < nBlocks; i++) {
            long k1 = getBlock64Uncheck(data, start + (i * 2) * 8);
            long k2 = getBlock64Uncheck(data, start + (i * 2 + 1) * 8);

            k1 *= c1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= c2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }
    }

    private void finalHashThenStore(int totalLen) {
        h1 ^= totalLen;
        h2 ^= totalLen;
        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);
        h1 += h2;
        h2 += h1;

        result.result1 = h1;
        result.result2 = h2;
    }

    @Override
    public HashResult128 hashLong(long value) {
        // avoid copying new bytes
        this.pos = 0;
        putInt64ToBuffer(value);
        processByBlock64(buffer, 0, pos);
        return this.result;
    }

    private long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    @Override
    public HashResult128 hashBytes(byte[] bytes) {
        processByBlock64(bytes, 0, bytes.length);
        return this.result;
    }
}