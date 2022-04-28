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

package com.alibaba.polardbx.server.util;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.alibaba.polardbx.net.util.CharsetUtil;
import com.alibaba.polardbx.common.utils.Pair;

/**
 * @author xianmao.hexm
 */
public final class StringUtil {

    private static final int      DEFAULT_INDEX      = -1;
    private static final String[] EMPTY_STRING_ARRAY = new String[0];
    private static final byte[]   EMPTY_BYTE_ARRAY   = new byte[0];
    private static final char[]   chars              = { '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'q', 'w',
            'e', 'r', 't', 'y', 'u', 'i', 'o', 'p', 'a', 's', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'z', 'x', 'c', 'v',
            'b', 'n', 'm', 'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P', 'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K',
            'L', 'Z', 'X', 'C', 'V', 'B', 'N', 'M'  };
    private static final Random   RANDOM             = new Random();

    public static String getRandomString(int size) {
        StringBuilder s = new StringBuilder(size);
        int len = chars.length;
        for (int i = 0; i < size; i++) {
            int x = RANDOM.nextInt();
            s.append(chars[(x < 0 ? -x : x) % len]);
        }
        return s.toString();
    }

    public static String safeToString(Object object) {
        try {
            return object.toString();
        } catch (Throwable t) {
            return "<toString() failure: " + t + ">";
        }
    }

    public static boolean isEmpty(String str) {
        return ((str == null) || (str.length() == 0));
    }

    public static byte[] hexString2Bytes(char[] hexString, int offset, int length) {
        if (hexString == null) return null;
        if (length == 0) return EMPTY_BYTE_ARRAY;
        boolean odd = length << 31 == Integer.MIN_VALUE;
        byte[] bs = new byte[odd ? (length + 1) >> 1 : length >> 1];
        for (int i = offset, limit = offset + length; i < limit; ++i) {
            char high, low;
            if (i == offset && odd) {
                high = '0';
                low = hexString[i];
            } else {
                high = hexString[i];
                low = hexString[++i];
            }
            int b;
            switch (high) {
                case '0':
                    b = 0;
                    break;
                case '1':
                    b = 0x10;
                    break;
                case '2':
                    b = 0x20;
                    break;
                case '3':
                    b = 0x30;
                    break;
                case '4':
                    b = 0x40;
                    break;
                case '5':
                    b = 0x50;
                    break;
                case '6':
                    b = 0x60;
                    break;
                case '7':
                    b = 0x70;
                    break;
                case '8':
                    b = 0x80;
                    break;
                case '9':
                    b = 0x90;
                    break;
                case 'a':
                case 'A':
                    b = 0xa0;
                    break;
                case 'b':
                case 'B':
                    b = 0xb0;
                    break;
                case 'c':
                case 'C':
                    b = 0xc0;
                    break;
                case 'd':
                case 'D':
                    b = 0xd0;
                    break;
                case 'e':
                case 'E':
                    b = 0xe0;
                    break;
                case 'f':
                case 'F':
                    b = 0xf0;
                    break;
                default:
                    throw new IllegalArgumentException("illegal hex-string: " + new String(hexString, offset, length));
            }
            switch (low) {
                case '0':
                    break;
                case '1':
                    b += 1;
                    break;
                case '2':
                    b += 2;
                    break;
                case '3':
                    b += 3;
                    break;
                case '4':
                    b += 4;
                    break;
                case '5':
                    b += 5;
                    break;
                case '6':
                    b += 6;
                    break;
                case '7':
                    b += 7;
                    break;
                case '8':
                    b += 8;
                    break;
                case '9':
                    b += 9;
                    break;
                case 'a':
                case 'A':
                    b += 10;
                    break;
                case 'b':
                case 'B':
                    b += 11;
                    break;
                case 'c':
                case 'C':
                    b += 12;
                    break;
                case 'd':
                case 'D':
                    b += 13;
                    break;
                case 'e':
                case 'E':
                    b += 14;
                    break;
                case 'f':
                case 'F':
                    b += 15;
                    break;
                default:
                    throw new IllegalArgumentException("illegal hex-string: " + new String(hexString, offset, length));
            }
            bs[(i - offset) >> 1] = (byte) b;
        }
        return bs;
    }

    public static String dumpAsHex(byte[] src, int length) {
        StringBuilder out = new StringBuilder(length * 4);
        int p = 0;
        int rows = length / 8;
        for (int i = 0; (i < rows) && (p < length); i++) {
            int ptemp = p;
            for (int j = 0; j < 8; j++) {
                String hexVal = Integer.toHexString(src[ptemp] & 0xff);
                if (hexVal.length() == 1) out.append('0');
                out.append(hexVal).append(' ');
                ptemp++;
            }
            out.append("    ");
            for (int j = 0; j < 8; j++) {
                int b = 0xff & src[p];
                if (b > 32 && b < 127) {
                    out.append((char) b).append(' ');
                } else {
                    out.append(". ");
                }
                p++;
            }
            out.append('\n');
        }
        int n = 0;
        for (int i = p; i < length; i++) {
            String hexVal = Integer.toHexString(src[i] & 0xff);
            if (hexVal.length() == 1) out.append('0');
            out.append(hexVal).append(' ');
            n++;
        }
        for (int i = n; i < 8; i++) {
            out.append("   ");
        }
        out.append("    ");
        for (int i = p; i < length; i++) {
            int b = 0xff & src[i];
            if (b > 32 && b < 127) {
                out.append((char) b).append(' ');
            } else {
                out.append(". ");
            }
        }
        out.append('\n');
        return out.toString();
    }

    public static byte[] escapeEasternUnicodeByteStream(byte[] src, String srcString, int offset, int length) {
        if ((src == null) || (src.length == 0)) return src;
        int bytesLen = src.length;
        int bufIndex = 0;
        int strIndex = 0;
        ByteArrayOutputStream out = new ByteArrayOutputStream(bytesLen);
        while (true) {
            if (srcString.charAt(strIndex) == '\\') {// write it out as-is
                out.write(src[bufIndex++]);
            } else {// Grab the first byte
                int loByte = src[bufIndex];
                if (loByte < 0) loByte += 256; // adjust for
                                               // signedness/wrap-around
                out.write(loByte);// We always write the first byte
                if (loByte >= 0x80) {
                    if (bufIndex < (bytesLen - 1)) {
                        int hiByte = src[bufIndex + 1];
                        if (hiByte < 0) hiByte += 256; // adjust for
                                                       // signedness/wrap-around
                        out.write(hiByte);// write the high byte here, and
                                          // increment the index for the high
                                          // byte
                        bufIndex++;
                        if (hiByte == 0x5C) out.write(hiByte);// escape 0x5c if
                                                              // necessary
                    }
                } else if (loByte == 0x5c) {
                    if (bufIndex < (bytesLen - 1)) {
                        int hiByte = src[bufIndex + 1];
                        if (hiByte < 0) hiByte += 256; // adjust for
                                                       // signedness/wrap-around
                        if (hiByte == 0x62) {// we need to escape the 0x5c
                            out.write(0x5c);
                            out.write(0x62);
                            bufIndex++;
                        }
                    }
                }
                bufIndex++;
            }
            if (bufIndex >= bytesLen) break;// we're done
            strIndex++;
        }
        return out.toByteArray();
    }

    public static String toString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "";
        StringBuffer buffer = new StringBuffer();
        for (byte byt : bytes) {
            buffer.append((char) byt);
        }
        return buffer.toString();
    }

    public static boolean equals(String str1, String str2) {
        if (str1 == null) {
            return str2 == null;
        }
        return str1.equals(str2);
    }

    public static boolean equalsIgnoreCase(String str1, String str2) {
        if (str1 == null) {
            return str2 == null;
        }
        return str1.equalsIgnoreCase(str2);
    }

    public static int countChar(String str, char c) {
        if (str == null || str.isEmpty()) return 0;
        final int len = str.length();
        int cnt = 0;
        for (int i = 0; i < len; ++i) {
            if (c == str.charAt(i)) {
                ++cnt;
            }
        }
        return cnt;
    }

    public static String[] split(String src) {
        return split(src, null, -1);
    }

    public static String[] split(String src, char separatorChar) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new ArrayList<String>();
        int i = 0;
        int start = 0;
        boolean match = false;
        while (i < length) {
            if (src.charAt(i) == separatorChar) {
                if (match) {
                    list.add(src.substring(start, i));
                    match = false;
                }
                start = ++i;
                continue;
            }
            match = true;
            i++;
        }
        if (match) {
            list.add(src.substring(start, i));
        }
        return list.toArray(new String[list.size()]);
    }

    public static String[] split(String src, char separatorChar, boolean trim) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new ArrayList<String>();
        int i = 0;
        int start = 0;
        boolean match = false;
        while (i < length) {
            if (src.charAt(i) == separatorChar) {
                if (match) {
                    if (trim) {
                        list.add(src.substring(start, i).trim());
                    } else {
                        list.add(src.substring(start, i));
                    }
                    match = false;
                }
                start = ++i;
                continue;
            }
            match = true;
            i++;
        }
        if (match) {
            if (trim) {
                list.add(src.substring(start, i).trim());
            } else {
                list.add(src.substring(start, i));
            }
        }
        return list.toArray(new String[list.size()]);
    }

    public static String[] split(String str, String separatorChars) {
        return split(str, separatorChars, -1);
    }

    public static String[] split(String src, String separatorChars, int max) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new ArrayList<String>();
        int sizePlus1 = 1;
        int i = 0;
        int start = 0;
        boolean match = false;
        if (separatorChars == null) {// null表示使用空白作为分隔符
            while (i < length) {
                if (Character.isWhitespace(src.charAt(i))) {
                    if (match) {
                        if (sizePlus1++ == max) {
                            i = length;
                        }
                        list.add(src.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                match = true;
                i++;
            }
        } else if (separatorChars.length() == 1) {// 优化分隔符长度为1的情形
            char sep = separatorChars.charAt(0);
            while (i < length) {
                if (src.charAt(i) == sep) {
                    if (match) {
                        if (sizePlus1++ == max) {
                            i = length;
                        }
                        list.add(src.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                match = true;
                i++;
            }
        } else {// 一般情形
            while (i < length) {
                if (separatorChars.indexOf(src.charAt(i)) >= 0) {
                    if (match) {
                        if (sizePlus1++ == max) {
                            i = length;
                        }
                        list.add(src.substring(start, i));
                        match = false;
                    }
                    start = ++i;
                    continue;
                }
                match = true;
                i++;
            }
        }
        if (match) {
            list.add(src.substring(start, i));
        }
        return list.toArray(new String[list.size()]);
    }

    /**
     * 解析字符串，比如: <br>
     * 1. c1='$',c2='-',c3='[',c4=']' 输入字符串：mysql_db$0-2<br>
     * 输出mysql_db[0],mysql_db[1],mysql_db[2]<br>
     * 2. c1='$',c2='-',c3='#',c4='0' 输入字符串：mysql_db$0-2<br>
     * 输出mysql_db#0,mysql_db#1,mysql_db#2<br>
     * 3. c1='$',c2='-',c3='0',c4='0' 输入字符串：mysql_db$0-2<br>
     * 输出mysql_db0,mysql_db1,mysql_db2<br>
     */
    public static String[] split(String src, char c1, char c2, char c3, char c4) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new ArrayList<String>();
        if (src.indexOf(c1) == -1) {
            list.add(src.trim());
        } else {
            String[] s = split(src, c1, true);
            String[] scope = split(s[1], c2, true);
            int min = Integer.parseInt(scope[0]);
            int max = Integer.parseInt(scope[scope.length - 1]);
            if (c3 == '0') {
                for (int x = min; x <= max; x++) {
                    list.add(new StringBuilder(s[0]).append(x).toString());
                }
            } else if (c4 == '0') {
                for (int x = min; x <= max; x++) {
                    list.add(new StringBuilder(s[0]).append(c3).append(x).toString());
                }
            } else {
                for (int x = min; x <= max; x++) {
                    list.add(new StringBuilder(s[0]).append(c3).append(x).append(c4).toString());
                }
            }
        }
        return list.toArray(new String[list.size()]);
    }

    public static String[] split(String src, char fi, char se, char th) {
        return split(src, fi, se, th, '0', '0');
    }

    public static String[] split(String src, char fi, char se, char th, char left, char right) {
        List<String> list = new ArrayList<String>();
        String[] pools = split(src, fi, true);
        for (int i = 0; i < pools.length; i++) {
            if (pools[i].indexOf(se) == -1) {
                list.add(pools[i]);
                continue;
            }
            String[] s = split(pools[i], se, th, left, right);
            for (int j = 0; j < s.length; j++) {
                list.add(s[j]);
            }
        }
        return list.toArray(new String[list.size()]);
    }

    public static String replaceOnce(String text, String repl, String with) {
        return replace(text, repl, with, 1);
    }

    public static String replace(String text, String repl, String with) {
        return replace(text, repl, with, -1);
    }

    public static String replace(String text, String repl, String with, int max) {
        if ((text == null) || (repl == null) || (with == null) || (repl.length() == 0) || (max == 0)) {
            return text;
        }
        StringBuffer buf = new StringBuffer(text.length());
        int start = 0;
        int end = 0;
        while ((end = text.indexOf(repl, start)) != -1) {
            buf.append(text.substring(start, end)).append(with);
            start = end + repl.length();
            if (--max == 0) {
                break;
            }
        }
        buf.append(text.substring(start));
        return buf.toString();
    }

    public static String replaceChars(String str, char searchChar, char replaceChar) {
        if (str == null) {
            return null;
        }
        return str.replace(searchChar, replaceChar);
    }

    public static String replaceChars(String str, String searchChars, String replaceChars) {
        if ((str == null) || (str.length() == 0) || (searchChars == null) || (searchChars.length() == 0)) {
            return str;
        }
        char[] chars = str.toCharArray();
        int len = chars.length;
        boolean modified = false;
        for (int i = 0, isize = searchChars.length(); i < isize; i++) {
            char searchChar = searchChars.charAt(i);
            if ((replaceChars == null) || (i >= replaceChars.length())) {// 删除
                int pos = 0;
                for (int j = 0; j < len; j++) {
                    if (chars[j] != searchChar) {
                        chars[pos++] = chars[j];
                    } else {
                        modified = true;
                    }
                }
                len = pos;
            } else {// 替换
                for (int j = 0; j < len; j++) {
                    if (chars[j] == searchChar) {
                        chars[j] = replaceChars.charAt(i);
                        modified = true;
                    }
                }
            }
        }
        if (!modified) {
            return str;
        }
        return new String(chars, 0, len);
    }

    /**
     * 字符串hash算法：s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1] <br>
     * 其中s[]为字符串的字符数组，换算成程序的表达式为：<br>
     * h = 31*h + s.charAt(i); => h = (h << 5) - h + s.charAt(i); <br>
     * 
     * @param start hash for s.substring(start, end)
     * @param end hash for s.substring(start, end)
     */
    public static long hash(String s, int start, int end) {
        if (start < 0) {
            start = 0;
        }
        if (end > s.length()) {
            end = s.length();
        }
        long h = 0;
        for (int i = start; i < end; ++i) {
            h = (h << 5) - h + s.charAt(i);
        }
        return h;
    }

    /**
     * "2" -&gt; (0,2)<br/>
     * "1:2" -&gt; (1,2)<br/>
     * "1:" -&gt; (1,0)<br/>
     * "-1:" -&gt; (-1,0)<br/>
     * ":-1" -&gt; (0,-1)<br/>
     * ":" -&gt; (0,0)<br/>
     */
    public static Pair<Integer, Integer> sequenceSlicing(String slice) {
        int ind = slice.indexOf(':');
        if (ind < 0) {
            int i = Integer.parseInt(slice.trim());
            if (i >= 0) {
                return new Pair<Integer, Integer>(0, i);
            } else {
                return new Pair<Integer, Integer>(i, 0);
            }
        }
        String left = slice.substring(0, ind).trim();
        String right = slice.substring(1 + ind).trim();
        int start, end;
        if (left.length() <= 0) {
            start = 0;
        } else {
            start = Integer.parseInt(left);
        }
        if (right.length() <= 0) {
            end = 0;
        } else {
            end = Integer.parseInt(right);
        }
        return new Pair<Integer, Integer>(start, end);
    }

    /**
     * <pre>
     * 将名字和索引用进行分割 当src = "offer_group[4]", l='[', r=']'时，
     * 返回的Piar<String,Integer>("offer", 4);
     * 当src = "offer_group", l='[', r=']'时， 
     * 返回Pair<String, Integer>("offer",-1);
     * </pre>
     */
    public static Pair<String, Integer> splitIndex(String src, char l, char r) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return new Pair<String, Integer>("", DEFAULT_INDEX);
        }
        if (src.charAt(length - 1) != r) {
            return new Pair<String, Integer>(src, DEFAULT_INDEX);
        }
        int offset = src.lastIndexOf(l);
        if (offset == -1) {
            return new Pair<String, Integer>(src, DEFAULT_INDEX);
        }
        int index = DEFAULT_INDEX;
        try {
            index = Integer.parseInt(src.substring(offset + 1, length - 1));
        } catch (NumberFormatException e) {
            return new Pair<String, Integer>(src, DEFAULT_INDEX);
        }
        return new Pair<String, Integer>(src.substring(0, offset), index);
    }

    /**
     * 解析字符串<br>
     * 比如:c1='$',c2='-' 输入字符串：mysql_db$0-2<br>
     * 输出array:mysql_db[0],mysql_db[1],mysql_db[2]
     */
    public static String[] split2(String src, char c1, char c2) {
        if (src == null) {
            return null;
        }
        int length = src.length();
        if (length == 0) {
            return EMPTY_STRING_ARRAY;
        }
        List<String> list = new ArrayList<String>();
        String[] p = split(src, c1, true);
        if (p.length > 1) {
            String[] scope = split(p[1], c2, true);
            int min = Integer.parseInt(scope[0]);
            int max = Integer.parseInt(scope[scope.length - 1]);
            for (int x = min; x <= max; x++) {
                list.add(new StringBuilder(p[0]).append('[').append(x).append(']').toString());
            }
        } else {
            list.add(p[0]);
        }
        return list.toArray(new String[list.size()]);
    }

    public static byte[] encode(String src, String charset) {
        if (src == null) {
            return null;
        }

        charset = CharsetUtil.getJavaCharset(charset);
        try {
            return src.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }

    public static byte[] encode_0(String src, String javaCharset) {
        if (src == null) {
            return null;
        }

        try {
            return src.getBytes(javaCharset);
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }

    public static String decode(byte[] src, String charset) {
        return decode(src, 0, src.length, charset);
    }

    public static String decode(byte[] src, int offset, int length, String charset) {
        try {
            return new String(src, offset, length, charset);
        } catch (UnsupportedEncodingException e) {
            return new String(src, offset, length);
        }
    }

    public static boolean isAscii(String str) {
        /* 检查是否包含非ascii字符 */
        char[] chars = str.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            byte[] bytes = ("" + chars[i]).getBytes();
            if (bytes.length > 1) {
                return false;
            }
        }

        return true;
    }

}
