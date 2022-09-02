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

package com.alibaba.polardbx.qatest.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

public final class RandomUtils {

    private RandomUtils() {

    }

    private static Random rd = new Random();
    private static int UNICODE_START = '\u4E00';
    private static int UNICODE_END = '\u9FA0';

    /**
     * 获取一个Long型整数，包括负数
     */
    public static long getLong() {
        return rd.nextLong();
    }

    /**
     * 获取一个大于0的Long型整数
     */
    public static long getLongMoreThanZero() {
        long res = rd.nextLong();
        while (res <= 0) {
            res = rd.nextLong();
        }
        return res;
    }

    /**
     * 获取一个小于某个值n的Long型整数，包括负数(负数的绝对值小于n)
     */
    public static long getLongLessThan(long n) {
        long res = rd.nextLong();
        return (res % n);
    }

    /**
     * 获取一个小于某个值n，并且大于0的Long型整数
     */
    public static long getLongMoreThanZeroLessThan(long n) {
        long res = getLongLessThan(n);
        while (res <= 0) {
            res = getLongLessThan(n);
        }
        return res;
    }

    /**
     * 获取一个小于某个值n，并且大于某个值m的Long型整数
     */
    public static long getLongBetween(long n, long m) {
        if (m <= n) {
            return n;
        }
        long res = getLongMoreThanZero();
        return (n + res % (m - n));
    }

    /**
     * 获取一个Integer型整数，包括负数
     */
    public static int getInteger() {
        return rd.nextInt();
    }

    /**
     * 获取一个大于0的Integer型整数
     */
    public static int getIntegerMoreThanZero() {
        int res = rd.nextInt();
        while (res <= 0) {
            res = rd.nextInt();
        }
        return res;
    }

    /**
     * 获取一个小于某个值n的Integer型整数，包括负数(负数的绝对值小于n)
     */
    public static int getIntegerLessThan(int n) {
        int res = rd.nextInt();
        return (res % n);
    }

    /**
     * 获取一个小于某个值n，并且大于0的Integer型整数
     */
    public static int getIntegerMoreThanZeroLessThan(int n) {
        int res = rd.nextInt(n);
        while (res == 0) {
            res = rd.nextInt(n);
        }
        return res;
    }

    /**
     * 获取一个小于某个值n，并且大于某个值m的Integer型整数
     */
    public static int getIntegerBetween(int n, int m)// m的值可能为负呢？
    {
        if (m == n) {
            return n;
        }
        int res = getIntegerMoreThanZero();
        return (n + res % (m - n));
    }

    private static char getChar(int arg[]) {
        int size = arg.length;
        // 判断生成字母还是数字
        int c = rd.nextInt(size / 2);
        c = c * 2;
        return (char) (getIntegerBetween(arg[c], arg[c + 1]));
    }

    private static String getString(int n, int arg[]) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < n; i++) {
            res.append(getChar(arg));
        }
        return res.toString();
    }

    /**
     * 获取一个指定长度，由字母组成的字符串
     */
    public static String getStringWithCharacter(int n) {
        int arg[] = new int[] {'a', 'z' + 1, 'A', 'Z' + 1};
        return getString(n, arg);
    }

    public static String getStringWithNumber(int n) {
        int arg[] = new int[] {'0', '9' + 1};
        return getString(n, arg);
    }

    /**
     * 获取一个指定长度，由数字和字母组成的字符串
     */
    public static String getStringWithNumAndCha(int n) {
        int arg[] = new int[] {'a', 'z' + 1, 'A', 'Z' + 1, '0', '9' + 1};
        return getString(n, arg);
    }

    /**
     * 获取一个长度比指定值小，由字母组成的字符串
     */
    public static String getStringShortenThan(int n) {
        int len = getIntegerMoreThanZeroLessThan(n);
        return getStringWithCharacter(len);
    }

    /**
     * 获取一个长度比指定值小，由数字和字母组成的字符串
     */
    public static String getStringWithNumAndChaShortenThan(int n) {
        int len = getIntegerMoreThanZeroLessThan(n);
        return getStringWithNumAndCha(len);
    }

    /**
     * 获取一个长度在最大值m与最小值n之间的字符串
     */
    public static String getStringBetween(int n, int m) {
        int len = getIntegerBetween(n, m);
        return getStringWithCharacter(len);
    }

    /**
     * 获取一个长度在最大值m与最小值n之间,由数字和字母组成的字符串
     */
    public static String getStringWithNumAndChaBetween(int n, int m) {
        int len = getIntegerBetween(n, m);
        return getStringWithNumAndCha(len);
    }

    /**
     * 获取一个具有指定前缀，并且长度是n的字符串。前缀的长度大于等于n时，返回前缀
     */
    public static String getStringWithPrefix(int n, String prefix) {
        int len = prefix.length();
        if (n <= len) {
            return prefix;
        } else {
            len = n - len;
            StringBuilder res = new StringBuilder(prefix);
            res.append(getStringWithCharacter(len));
            return res.toString();
        }
    }

    /**
     * 获取一个具有指定后缀，并且长度是n的字符串。后缀的长度大于等于n时，返回后缀
     */
    public static String getStringWithSuffix(int n, String suffix) {

        int len = suffix.length();
        if (n <= len) {
            return suffix;
        } else {
            len = n - len;
            StringBuilder res = new StringBuilder();
            res.append(getStringWithCharacter(len));
            res.append(suffix);
            return res.toString();
        }
    }

    /**
     * 获取一个具有指定前缀和后缀，并且长度是n的字符串。后缀的长度大于等于n时，返回前缀和后缀的连接
     */
    public static String getStringWithBoth(int n, String prefix, String suffix) {
        int len = prefix.length() + suffix.length();
        StringBuilder res = new StringBuilder(prefix);
        if (n <= len) {
            return res.append(suffix).toString();
        } else {
            len = n - len;
            res.append(getStringWithCharacter(len));
            res.append(suffix);
            return res.toString();
        }
    }

    /**
     * 取得具有指定前缀、指定长度的中文字符
     */
    public static String getCheseWordWithPrifix(int n, String prefix) {
        int len = prefix.length();
        if (n <= len) {
            return prefix;
        } else {
            len = n - len;
            StringBuilder res = new StringBuilder(prefix);
            res.append(getCheseWord(len));
            return res.toString();
        }
    }

    /**
     * 取得具有指定后缀、指定长度的中文字符
     */
    public static String getCheseWordWithSuffix(int n, String suffix) {

        int len = suffix.length();
        if (n <= len) {
            return suffix;
        } else {
            len = n - len;
            StringBuilder res = new StringBuilder();
            res.append(getCheseWord(len));
            res.append(suffix);
            return res.toString();
        }
    }

    /**
     * 获取一个具有指定前缀和后缀，并且长度是n的中文字符串。后缀的长度大于等于n时，返回前缀和后缀的连接
     */
    public static String getCheseWordWithBoth(int n, String prefix,
                                              String suffix) {
        int len = prefix.length() + suffix.length();
        StringBuilder res = new StringBuilder(prefix);
        if (n <= len) {
            return res.append(suffix).toString();
        } else {
            len = n - len;
            res.append(getCheseWord(len));
            res.append(suffix);
            return res.toString();
        }
    }

    /**
     * 取得指定长度的中文字符
     */
    public static String getCheseWord(int len) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < len; i++) {
            char str = getCheseChar();
            res.append(str);
        }
        return res.toString();
    }

    /**
     * 取得一个中文字符
     */
    private static char getCheseChar() {
        return (char) (UNICODE_START + rd.nextInt(UNICODE_END - UNICODE_START));
    }

    /**
     * 取得一个boolean值
     */
    public static boolean getBoolean() {
        return getIntegerMoreThanZeroLessThan(3) == 1;
    }

    public static void main(String[] args) {
        for (int t = 0; t < 20; t++) {

            Collection<Integer> arrs = getRandomCollection(1, 5, 5);
            for (int i : arrs) {
                System.out.println(i);
            }
            System.out.println("----");
        }
    }

    /**
     * 取得UUID的字符串
     */
    public static String getStringByUUID() {
        return UUID.randomUUID().toString();
    }

    /**
     * 取得指定范围内的N个不重复的随机数(会申请比较大的内存，不太适合大范围使用，也不适合性能测试使用)。
     * 在初始化的无重复待选数组中随机产生一个数放入结果中， 将待选数组被随机到的数，用待选数组(len-1)下标对应的数替换
     * 然后从len-2里随机产生下一个随机数，如此类推
     *
     * @param max 指定范围最大值
     * @param min 指定范围最小值
     * @param n 随机数个数
     * @return List<Integer> 随机数结果集
     */
    public static int[] getRandomArray(int min, int max, int n) {
        int len = max - min + 1;

        if (max < min || n > len) {
            return null;
        }

        // 初始化给定范围的待选数组
        int[] source = new int[len];
        for (int i = min; i < min + len; i++) {
            source[i - min] = i;
        }

        int[] result = new int[n];
        Random rd = new Random();
        int index = 0;
        for (int i = 0; i < result.length; i++) {
            // 待选数组0到(len-2)随机一个下标
            index = Math.abs(rd.nextInt() % len--);
            // 将随机到的数放入结果集
            result[i] = source[index];
            // 将待选数组中被随机到的数，用待选数组(len-1)下标对应的数替换
            source[index] = source[len];
        }
        return result;
    }

    /**
     * 取得指定范围内的N个不重复的随机数(每次利用随机数生成，可以性能测试时使用)。
     *
     * @param max 指定范围最大值
     * @param min 指定范围最小值
     * @param n 随机数个数
     */
    public static Collection<Integer> getRandomCollection(int min, int max,
                                                          int n) {
        Set<Integer> res = new HashSet<Integer>();
        int mx = max;
        int mn = min;
        if (n == (max + 1 - min)) {
            for (int i = 1; i <= n; i++) {
                res.add(i);
            }
            return res;
        }
        for (int i = 0; i < n; i++) {
            int v = getIntegerBetween(mn, mx);
            if (v == mx) {
                mx--;
            }
            if (v == mn) {
                mn++;
            }
            while (res.contains(v)) {
                v = getIntegerBetween(mn, mx);
                if (v == mx) {
                    mx = v;
                }
                if (v == mn) {
                    mn = v;
                }
            }
            res.add(v);
        }
        return res;
    }
}