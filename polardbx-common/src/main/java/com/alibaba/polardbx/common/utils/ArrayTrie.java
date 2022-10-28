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

package com.alibaba.polardbx.common.utils;

import java.util.Arrays;
import java.util.Collection;

/**
 * 查找并发安全
 * 构造时非并发安全
 *
 * @see org.eclipse.jetty.util.ArrayTernaryTrie
 */
public class ArrayTrie {

    private static final char[] lowercases = {
        '\000', '\001', '\002', '\003', '\004', '\005', '\006', '\007',
        '\010', '\011', '\012', '\013', '\014', '\015', '\016', '\017',
        '\020', '\021', '\022', '\023', '\024', '\025', '\026', '\027',
        '\030', '\031', '\032', '\033', '\034', '\035', '\036', '\037',
        '\040', '\041', '\042', '\043', '\044', '\045', '\046', '\047',
        '\050', '\051', '\052', '\053', '\054', '\055', '\056', '\057',
        '\060', '\061', '\062', '\063', '\064', '\065', '\066', '\067',
        '\070', '\071', '\072', '\073', '\074', '\075', '\076', '\077',
        '\100', '\141', '\142', '\143', '\144', '\145', '\146', '\147',
        '\150', '\151', '\152', '\153', '\154', '\155', '\156', '\157',
        '\160', '\161', '\162', '\163', '\164', '\165', '\166', '\167',
        '\170', '\171', '\172', '\133', '\134', '\135', '\136', '\137',
        '\140', '\141', '\142', '\143', '\144', '\145', '\146', '\147',
        '\150', '\151', '\152', '\153', '\154', '\155', '\156', '\157',
        '\160', '\161', '\162', '\163', '\164', '\165', '\166', '\167',
        '\170', '\171', '\172', '\173', '\174', '\175', '\176', '\177'};

    private static final int LOW = 1;
    private static final int EQUAL = 2;
    private static final int HIGH = 3;
    private static final int ROW_SIZE = 4;

    private final boolean caseInsensitive;
    private final int capacity;
    private final char[] tree;
    private final long[] bitset;
    private int rowCount;

    public synchronized static ArrayTrie buildTrie(Collection<String> collection, boolean caseInsensitive, int capacity)
        throws Exception {
        ArrayTrie arrayTrie = new ArrayTrie(caseInsensitive, capacity);
        for (String s : collection) {
            if (!arrayTrie.add(s)) {
                throw new Exception("Array Trie overflow by capacity: " + capacity);
            }
        }
        return arrayTrie;
    }

    /**
     * @param capacity 需要根据共用前缀情况来决定容量大小
     */
    private ArrayTrie(boolean caseInsensitive, int capacity) {
        this.caseInsensitive = caseInsensitive;
        this.capacity = capacity;
        this.tree = new char[capacity * ROW_SIZE];
        this.bitset = new long[(int) Math.ceil((double) capacity / (double) Long.SIZE)];
        Arrays.fill(bitset, 0);
    }

    public static int highOrLow(int diff) {
        return 1 + (diff | Integer.MAX_VALUE) / (Integer.MAX_VALUE / 2);
    }

    public boolean add(String s) {
        if (s == null || s.length() == 0) {
            throw new IllegalArgumentException("cannot add empty String in ArrayTrie");
        }

        int t = 0, last = 0;
        int len = s.length();
        for (int k = 0; k < len; k++) {
            char ch = s.charAt(k);
            if (this.caseInsensitive && ch < 128) {
                ch = lowercases[ch];
            }

            while (true) {
                int row = ROW_SIZE * t;

                if (t == rowCount) {
                    if (isFull()) {
                        return false;
                    }
                    rowCount++;
                    tree[row] = ch;
                }

                char n = tree[row];
                int diff = n - ch;
                if (diff == 0) {
                    last = row + EQUAL;
                } else if (diff < 0) {
                    last = row + LOW;
                } else {
                    last = row + HIGH;
                }

                t = tree[last];
                if (t == 0) {
                    t = rowCount;
                    tree[last] = (char) t;
                }

                if (diff == 0) {
                    break;
                }
            }
        }

        if (t == rowCount) {
            if (isFull()) {
                return false;
            }
            rowCount++;
        }

        setBit(t);
        return true;
    }

    public boolean contains(String s) {
        int t = 0, len = s.length();
        for (int i = 0; i < len; ) {
            char ch = s.charAt(i++);
            if (this.caseInsensitive && ch < 128) {
                ch = lowercases[ch];
            }

            while (true) {
                int row = ROW_SIZE * t;
                char n = tree[row];
                int diff = n - ch;

                if (diff == 0) {
                    t = tree[row + EQUAL];
                    if (t == 0) {
                        return false;
                    }
                    break;
                } else if (diff < 0) {
                    t = tree[row + LOW];
                } else {
                    t = tree[row + HIGH];
                }

                if (t == 0) {
                    return false;
                }
            }
        }

        return (bitset[t >>> 6] & (1L << t)) != 0;
    }

    private void setBit(int index) {
        int longIndex = (index >>> 6);
        long mask = 1L << index;
        bitset[longIndex] |= mask;
    }

    public int size() {
        return this.rowCount;
    }

    public boolean isFull() {
        return this.rowCount >= this.capacity - 1;
    }
}
