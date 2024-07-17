package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.partition.MurmurHashUtils;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import org.junit.Test;

/**
 * @author chenghui.lch
 */
public class MyMurmurHashTest {

    public MyMurmurHashTest() {

    }

    public static byte[] convertLongToBytes(long ts) {
        byte[] tsBytes = Longs.toByteArray(ts);
        return tsBytes;
    }

    /**
     * The hash sort impl of the collation of utf8mb4bin, Just For Test
     */
    public static long mysqlUtf8Mb4BinHashSort(byte[] fldValBytes, long[] nr1, long[] nr2) {
        long tmp1 = nr1[0];
        long tmp2 = nr2[0];

        if (fldValBytes == null) {
            tmp1 ^= (tmp1 << 1) | 1;
        } else {
            // skip trailing space
            int len = fldValBytes.length;
            while (len >= 1 && fldValBytes[len - 1] == 0x20) {
                len--;
            }

            for (int i = 0; i < len; i++) {
                byte b = fldValBytes[i];
                tmp1 ^= (((tmp1 & 0x3F) + tmp2) * Byte.toUnsignedInt(b)) + (tmp1 << 8);
                tmp2 += 3;
            }
        }
        nr1[0] = tmp1;
        nr2[0] = tmp2;
        return tmp1;
    }

    public HashFunction hf = Hashing.murmur3_128(0);

    public static long calcOneFieldHashCode(long valData,
                                            long[] seed1,
                                            long[] seed2) {
        byte[] bytes = convertLongToBytes(valData);
        mysqlUtf8Mb4BinHashSort(bytes, seed1, seed2);
        return seed1[0];
    }

    public long calcHashCode(long[] datas) {

        long outputHashCode = 1l;
        long[] seed1 = new long[1];
        long[] seed2 = new long[1];
        seed1[0] = 1;
        seed2[0] = 4;
        int partCnt = datas.length;
        int i = 0;
        do {
            long bndVal = datas[i];
            calcOneFieldHashCode(bndVal,
                seed1, seed2);
            ++i;
        } while (i < partCnt);
        outputHashCode = seed1[0];
        return outputHashCode;
    }

    @Test
    public void testMurmurHash2() {

        int seed = 0;
        long st = 1000000;
        long testCnt = 1000;
        int pCnt = 4;
        long rangeSize = Long.MAX_VALUE / pCnt;
        rangeSize = rangeSize * 2;
        long[] rangeArr = new long[pCnt];
        long[] rangeStat = new long[pCnt];

        long upPoint = Long.MIN_VALUE;
        for (int p = 0; p < pCnt; p++) {
            upPoint = upPoint + rangeSize;
            rangeArr[p] = upPoint;
            rangeStat[p] = 0;
        }
        int colCnt = 1;
        long sts = System.nanoTime();
        for (int i = 0; i < testCnt; i++) {
            long shardVal = st + i;
            long[] datas = new long[colCnt];
            for (int j = 0; j < colCnt; j++) {
                datas[j] = shardVal;
            }
            long val = MurmurHashUtils.murmurHash128WithZeroSeed(calcHashCode(datas));
            for (int j = 0; j < pCnt; j++) {
                if (val < rangeArr[j]) {
                    ++rangeStat[j];
                    break;
                }
            }
        }

        long ets = System.nanoTime();
        System.out.print("test2:\n");
        System.out.print(String.format("tc:%d\n", (ets - sts) / testCnt));
        for (int i = 0; i < pCnt; i++) {
            System.out.print(String
                .format("p%s-ratio:%s%%, cnt:%s, upBound:%s\n", i, Math.round((rangeStat[i] * 100.0) / testCnt),
                    rangeStat[i], rangeArr[i]));
        }
    }

    @Test
    public void testMurmurHash() {

        int seed = 0;
        long st = 0;
        long testCnt = 100000;
        int pCnt = 16;
        long rangeSize = Long.MAX_VALUE / pCnt;
        rangeSize = rangeSize * 2;
        long[] rangeArr = new long[pCnt];
        long[] rangeStat = new long[pCnt];

        long upPoint = Long.MIN_VALUE;
        for (int p = 0; p < pCnt; p++) {
            upPoint = upPoint + rangeSize;
            rangeArr[p] = upPoint;
            rangeStat[p] = 0;
        }
        HashFunction hf = Hashing.murmur3_128(seed);

        long sts = System.nanoTime();
        for (int i = 0; i < testCnt; i++) {
            long shardVal = st + i;
            HashCode hc = hf.hashLong(shardVal * 100);
            long val = hc.asLong();
            for (int j = 0; j < pCnt; j++) {
                if (val < rangeArr[j]) {
                    ++rangeStat[j];
                    break;
                }
            }
        }
        long ets = System.nanoTime();
        System.out.print(String.format("tc:%d\n", (ets - sts) / testCnt));
        for (int i = 0; i < pCnt; i++) {
            System.out.print(String
                .format("p%s-ratio:%s%%, cnt:%s, upPoint:%s\n", i, Math.round((rangeStat[i] * 100.0) / testCnt),
                    rangeStat[i], rangeArr[i]));
        }
    }

    public static int hash32(byte[] data, int seed) {
        int h1 = seed;
        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;
        int len = data.length;
        int i = 0;
        while (len >= 4) {
            int k1 = (data[i++] & 0xff) | ((data[i++] & 0xff) << 8) | ((data[i++] & 0xff) << 16) | (data[i++] << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
            len -= 4;
        }
        int k1 = 0;
        switch (len) {
        case 3:
            k1 ^= (data[i + 2] & 0xff) << 16;
        case 2:
            k1 ^= (data[i + 1] & 0xff) << 8;
        case 1:
            k1 ^= (data[i] & 0xff);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
        }
        h1 ^= data.length;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }

    public static long zeroSeedMurmurHash3Hash32(byte[] data, int seed) {
        return hash32(data, 0);
    }

    public static long doMurmurHash(String str) {
        return zeroSeedMurmurHash3Hash64(str.getBytes());
    }

    public static long doMurmurHash(Long val) {
        return zeroSeedMurmurHash3Hash64(longToBytes(val));
    }

    public static byte[] longToBytes(long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return result;
    }

    public static long zeroSeedMurmurHash3Hash64(byte[] data) {
        return hash64(data, 0);
    }

    public static long hash64(byte[] data, int seed) {
        int len = data.length;
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;
        long h = (seed & 0xffffffffl) ^ (len * m);
        int i = 0;
        while (len >= 8) {
            long k =
                ((long) data[i] & 0xff) + (((long) data[i + 1] & 0xff) << 8) + (((long) data[i + 2] & 0xff) << 16) + (
                    ((long) data[i + 3] & 0xff) << 24) + (((long) data[i + 4] & 0xff) << 32) + (
                    ((long) data[i + 5] & 0xff) << 40) + (((long) data[i + 6] & 0xff) << 48) + (
                    ((long) data[i + 7] & 0xff) << 56);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h ^= k;
            h *= m;
            i += 8;
            len -= 8;
        }
        switch (len) {
        case 7:
            h ^= (long) (data[i + 6] & 0xff) << 48;
        case 6:
            h ^= (long) (data[i + 5] & 0xff) << 40;
        case 5:
            h ^= (long) (data[i + 4] & 0xff) << 32;
        case 4:
            h ^= (long) (data[i + 3] & 0xff) << 24;
        case 3:
            h ^= (long) (data[i + 2] & 0xff) << 16;
        case 2:
            h ^= (long) (data[i + 1] & 0xff) << 8;
        case 1:
            h ^= (long) (data[i] & 0xff);
            h *= m;
        }
        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        return h;
    }
}
