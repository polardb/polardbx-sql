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

package com.alibaba.polardbx.executor.statistic.ndv;

import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableNDVSketchStatistic;
import io.airlift.slice.XxHash64;

import java.util.BitSet;

public class HyperLogLogUtil {

    public static int HLL_REGISTERS = 16384;
    public static int HLL_REGBYTES = HLL_REGISTERS * 6 / 8;
    public static int HLL_REGBYTES_DE = HLL_REGBYTES + 1;
    public static int HLL_BITS = 6;
    public static int HLL_REGISTER_MAX = ((1 << HLL_BITS) - 1);
    public static int HLL_P_MASK = HLL_REGISTERS - 1;
    public static int HLL_P = 14;
    public static int HLL_Q = 64 - HLL_P;
    public static double HLL_ALPHA_INF = 0.721347520444481703680;/* constant for 0.5/ln(2) */

    public static void hllSet(byte[] r, long ele) {
        // hllPatLen
        long hash = XxHash64.hash(ele);
        int index = (int) (hash & HLL_P_MASK);

        hash = hash >>> HLL_P;
        hash = hash | (1L << HLL_Q);
        int count = 1;
        long bit = 1;
        while ((hash & bit) == 0) {
            count++;
            bit <<= 1;
        }

        int oldCount = get(r, index);
        if (oldCount < count) {
            set(r, index, count);
        }
    }

    public static void merge(byte[] r, byte[] tmp) {
        BitSet bitSet = BitSet.valueOf(tmp);
        for (int i = 0; i < HLL_REGISTERS; i++) {
            int rValue = get(r, i);
            int tmpValue = bitToInt(bitSet, i * 6);
            if (rValue < tmpValue) {
                set(r, i, tmpValue);
            }
        }
    }

    private static int get(byte[] a, int pos) {
        int bytePos = pos * HLL_BITS / 8;
        int bitRemine = (pos * HLL_BITS) & 7;
        return (((a[bytePos] & 0xFF) >>> bitRemine) | (a[bytePos + 1] << (8 - bitRemine))) & HLL_REGISTER_MAX;
    }

    private static void set(byte[] a, int pos, int val) {
        int bytePos = pos * HLL_BITS / 8;
        int bitRemine = (pos * HLL_BITS) & 7;
        a[bytePos] &= ~(HLL_REGISTER_MAX << bitRemine);
        a[bytePos] |= (val << bitRemine);
        a[bytePos + 1] &= ~(HLL_REGISTER_MAX >> (8 - bitRemine));
        a[bytePos + 1] |= (val >> (8 - bitRemine));
    }

    public static int bitToInt(BitSet bitSet, int index) {
        BitSet b = bitSet.get(index, index + 5);
        char[] v = new char[6];
        v[5] = b.get(0) ? '1' : '0';
        v[4] = b.get(1) ? '1' : '0';
        v[3] = b.get(2) ? '1' : '0';
        v[2] = b.get(3) ? '1' : '0';
        v[1] = b.get(4) ? '1' : '0';
        v[0] = b.get(5) ? '1' : '0';
        Integer integer = Integer.valueOf(new String(v), 2);
        return integer;
    }

    public static double[] buildReghisto(int[] regValue) {
        double[] reghisto = new double[64];
        for (int i = 0; i < regValue.length; i++) {
            reghisto[regValue[i]]++;
        }
        return reghisto;
    }

    public static long reckon(double[] reghisto) {
        double z = HLL_REGISTERS * tau((HLL_REGISTERS - reghisto[HLL_Q + 1]) / (HLL_REGISTERS));
        for (int j = HLL_Q; j >= 1; --j) {
            z += reghisto[j];
            z *= 0.5;
        }
        z += HLL_REGISTERS * sigma(reghisto[0] / (double) HLL_REGISTERS);
        double result = Math.round(HLL_ALPHA_INF * HLL_REGISTERS * HLL_REGISTERS / z);

        return Math.round(result);
    }

    /* Helper function sigma as defined in
     * "New cardinality estimation algorithms for HyperLogLog sketches"
     * Otmar Ertl, arXiv:1702.01284 */
    static double sigma(double x) {
        if (x == 1.) {
            return -1;
        }
        double zPrime;
        double y = 1;
        double z = x;
        do {
            x *= x;
            zPrime = z;
            z += x * y;
            y += y;
        } while (zPrime != z);
        return z;
    }

    /* Helper function tau as defined in
     * "New cardinality estimation algorithms for HyperLogLog sketches"
     * Otmar Ertl, arXiv:1702.01284 */
    static double tau(double x) {
        if (x == 0. || x == 1.) {
            return 0.;
        }
        double zPrime;
        double y = 1.0;
        double z = 1 - x;
        do {
            x = Math.sqrt(x);
            zPrime = z;
            y *= 0.5;
            z -= Math.pow(1 - x, 2) * y;
        } while (zPrime != z);
        return z / 3;
    }

    public static String buildSketchKey(SystemTableNDVSketchStatistic.SketchRow sketchRow) {
        return buildSketchKey(sketchRow.getSchemaName(), sketchRow.getTableName(), sketchRow.getColumnNames());
    }

    public static String buildSketchKey(String schemaName, String tableName, String columnNames) {
        return (schemaName + ":" + tableName + ":" + columnNames).toLowerCase();
    }

    public static long getCardinality(byte[] bytes) {
        if (bytes == null) {
            return 0;
        }
        int[] registers = new int[HLL_REGISTERS];
        BitSet bitSet = BitSet.valueOf(bytes);
        for (int i = 0; i * 6 < HLL_REGBYTES * 8; i++) {// cal the reciprocal
            int v = bitToInt(bitSet, i * 6);
            registers[i] = v;
        }
        return reckon(buildReghisto(registers));
    }

    public static long estimate(byte[][] bytes) {
        byte[] m = bytes[0];
        if (m == null || m.length != HLL_REGBYTES) {
            throw new IllegalArgumentException();
        }

        int[] registers = new int[HLL_REGISTERS];
        for (int i = 0; i < bytes.length; i++) {
            BitSet bitSet = BitSet.valueOf(bytes[i]);
            for (int j = 0; j * 6 < m.length * 8; j++) {// cal the reciprocal
                int v = bitToInt(bitSet, j * 6);
                if (registers[j] < v) {
                    registers[j] = v;
                }
            }
        }

        return reckon(buildReghisto(registers));
    }
}
