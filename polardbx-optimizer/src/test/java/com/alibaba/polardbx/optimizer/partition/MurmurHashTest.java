/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.partition;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.alibaba.polardbx.common.partition.MurmurHashUtils;
import org.junit.Test;

/**
 * @author chenghui.lch
 */
public class MurmurHashTest {

    public MurmurHashTest() {

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
            long val = MurmurHashUtils.murmurHashWithZeroSeed(calcHashCode(datas));
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
}
