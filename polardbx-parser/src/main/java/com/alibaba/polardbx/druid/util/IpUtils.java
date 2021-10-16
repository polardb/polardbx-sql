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

package com.alibaba.polardbx.druid.util;

public class IpUtils {
    public static String ipInt2DotDec(int ipInt) throws IllegalArgumentException {
        return ipLong2DotDec(((long)ipInt) & 0xFFFFFFFFL);
    }

    public static String ipLong2DotDec(long ipLong) throws IllegalArgumentException {
        if (ipLong < 0) {
            throw new IllegalArgumentException("argument must be positive");
        } else if (ipLong > 4294967295L) {
            throw new IllegalArgumentException("argument must be smaller than 4294967295L");
        }

        StringBuffer sb = new StringBuffer(15);
        sb.append((int) (ipLong >>> 24));
        sb.append('.');
        sb.append((int) ((ipLong & 0x00FFFFFF) >>> 16));
        sb.append('.');
        sb.append((int) ((ipLong & 0x0000FFFF) >>> 8));
        sb.append('.');
        sb.append((int) ((ipLong & 0x000000FF)));
        return sb.toString();
    }

    static int number(char ch) {
        int val = ch - '0';
        if (val < 0 || val > 9) {
            throw new IllegalArgumentException();
        }
        return val;
    }

    public static int ipDotDec2Int(String ip) {
        if (ip == null || ip.length() == 0) {
            throw new IllegalArgumentException("argument is null");
        }
        
        if (ip.length() > 15) {
            throw new IllegalArgumentException("illegal ip : " + ip);
        }

        int i = 0;
        char ch = ip.charAt(i++);

        while (ch == ' ' && i < ip.length()) {
            ch = ip.charAt(i++);
        }

        int p0 = number(ch);

        ch = ip.charAt(i++);

        if (ch != '.') {
            p0 = p0 * 10 + number(ch);
            ch = ip.charAt(i++);
        }
        if (ch != '.') {
            p0 = p0 * 10 + number(ch);
            ch = ip.charAt(i++);
        }

        if (ch != '.') {
            throw new IllegalArgumentException("illegal ip : " + ip);
        }
        ch = ip.charAt(i++);

        int p1 = number(ch);
        ch = ip.charAt(i++);
        if (ch != '.') {
            p1 = p1 * 10 + number(ch);
            ch = ip.charAt(i++);
        }
        if (ch != '.') {
            p1 = p1 * 10 + number(ch);
            ch = ip.charAt(i++);
        }

        if (ch != '.') {
            throw new IllegalArgumentException("illegal ip : " + ip);
        }
        ch = ip.charAt(i++);

        int p2 = number(ch);
        ch = ip.charAt(i++);

        if (ch != '.') {
            p2 = p2 * 10 + number(ch);
            ch = ip.charAt(i++);
        }
        if (ch != '.') {
            p2 = p2 * 10 + number(ch);
            ch = ip.charAt(i++);
        }

        if (ch != '.') {
            throw new IllegalArgumentException("illegal ip : " + ip);
        }
        ch = ip.charAt(i++);

        int p3 = number(ch);
        if (i < ip.length()) {
            ch = ip.charAt(i++);
            p3 = p3 * 10 + number(ch);
        }
        if (i < ip.length()) {
            ch = ip.charAt(i++);
            p3 = p3 * 10 + number(ch);
        }

        return (p0 << 24) + (p1 << 16) + (p2 << 8) + p3;
    }

    public static long ipDotDec2Long(String ipDotDec) throws IllegalArgumentException {
        int intValue = ipDotDec2Int(ipDotDec);
        return ((long) intValue) & 0xFFFFFFFFL;
    }

    public static String ipHex2DotDec(String ipHex) throws IllegalArgumentException {
        if (ipHex == null) {
            throw new IllegalArgumentException("argument is null");
        }
        ipHex = ipHex.trim();
        if (ipHex.length() != 8 && ipHex.length() != 7) {
            throw new IllegalArgumentException("argument is: " + ipHex);
        }

        long ipLong = Long.parseLong(ipHex, 16);
        return ipLong2DotDec(ipLong);
    }

    public static String ipDotDec2Hex(String ipDotDec) throws IllegalArgumentException {
        long ipLong = ipDotDec2Long(ipDotDec);
        String ipHex = Long.toHexString(ipLong);
        if (ipHex.length() < 8) {
            ipHex = "0" + ipHex;
        }
        return ipHex;
    }
}
