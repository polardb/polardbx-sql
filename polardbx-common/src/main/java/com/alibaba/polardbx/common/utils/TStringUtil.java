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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TStringUtil extends StringUtils {

    private final static long[] pow10 = {
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
        10000000000L, 100000000000L, 1000000000000L, 10000000000000L, 100000000000000L, 1000000000000000L,
        10000000000000000L, 100000000000000000L, 1000000000000000000L};

    protected static final char[] HEX_CHAR = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
        'e', 'f'};


    public static String getBetween(String sql, String start, String end) {
        if (sql == null || start == null || end == null) {
            return null;
        }

        int index0 = sql.indexOf(start);
        if (index0 == -1) {
            return null;
        }
        int index1 = sql.indexOf(end, index0);
        if (index1 == -1) {
            return null;
        }
        return sql.substring(index0 + start.length(), index1).trim();
    }


    public static String removeBetween(String sql, String start, String end) {
        if (sql == null) {
            return null;
        }

        if (start == null || end == null) {
            return sql;
        }

        int index0 = sql.indexOf(start);
        if (index0 == -1) {
            return sql;
        }
        int index1 = sql.indexOf(end, index0);
        if (index1 == -1) {
            return sql;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(sql.substring(0, index0));
        sb.append(" ");
        sb.append(sql.substring(index1 + end.length()));
        return sb.toString();
    }


    public static String[] twoPartSplit(String str, String splitor) {
        if (str != null && splitor != null) {
            int index = str.indexOf(splitor);
            if (index != -1) {
                String first = str.substring(0, index);
                String sec = str.substring(index + splitor.length());
                return new String[] {first, sec};
            } else {
                return new String[] {str};
            }
        } else {
            return new String[] {str};
        }
    }


    public static List<String> recursiveSplit(String str, String splitor) {
        List<String> re = new ArrayList<String>();
        String[] strs = twoPartSplit(str, splitor);
        if (strs.length == 2) {
            re.add(strs[0]);
            re.addAll(recursiveSplit(strs[1], splitor));
        } else {
            re.add(strs[0]);
        }
        return re;
    }


    public static String fillTabWithSpace(String str) {
        if (str == null) {
            return null;
        }

        str = str.trim();
        int sz = str.length();
        StringBuilder buffer = new StringBuilder(sz);

        int index = 0, index0 = -1, index1 = -1;
        for (int i = 0; i < sz; i++) {
            char c = str.charAt(i);
            if (!Character.isWhitespace(c)) {
                if (index0 != -1) {

                    if (index0 != index1 || str.charAt(i - 1) != ' ') {
                        buffer.append(str.substring(index, index0)).append(" ");
                        index = index1 + 1;
                    }
                }
                index0 = index1 = -1;
            } else {
                if (index0 == -1) {
                    index0 = index1 = i;
                } else {
                    index1 = i;
                }
            }
        }

        buffer.append(str.substring(index));
        return buffer.toString();
    }


    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor) {
        return startsWithIgnoreCaseAndWs(searchIn, searchFor, 0);
    }


    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor, int beginPos) {
        if (searchIn == null) {
            return searchFor == null;
        }

        int inLength = searchIn.length();

        for (; beginPos < inLength; beginPos++) {
            if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                break;
            }
        }

        return startsWithIgnoreCase(searchIn, beginPos, searchFor);
    }


    public static boolean startsWithIgnoreCase(String searchIn, int startAt, String searchFor) {
        return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
    }


    public static String stripComments(String src, String stringOpens, String stringCloses, boolean slashStarComments,
                                       boolean slashSlashComments, boolean hashComments, boolean dashDashComments) {
        if (src == null) {
            return null;
        }

        StringBuffer buf = new StringBuffer(src.length());

        StringReader sourceReader = new StringReader(src);

        int contextMarker = Character.MIN_VALUE;
        boolean escaped = false;
        int markerTypeFound = -1;

        int ind = 0;

        int currentChar = 0;

        try {
            while ((currentChar = sourceReader.read()) != -1) {
                if (currentChar == '\\') {
                    escaped = !escaped;
                } else if (markerTypeFound != -1 && currentChar == stringCloses.charAt(markerTypeFound) && !escaped) {
                    contextMarker = Character.MIN_VALUE;
                    markerTypeFound = -1;
                } else if ((ind = stringOpens.indexOf(currentChar)) != -1 && !escaped
                    && contextMarker == Character.MIN_VALUE) {
                    markerTypeFound = ind;
                    contextMarker = currentChar;
                }

                if (contextMarker == Character.MIN_VALUE && currentChar == '/'
                    && (slashSlashComments || slashStarComments)) {
                    currentChar = sourceReader.read();
                    if (currentChar == '*' && slashStarComments) {
                        int prevChar = 0;
                        while ((currentChar = sourceReader.read()) != '/' || prevChar != '*') {
                            if (currentChar == '\r') {

                                currentChar = sourceReader.read();
                                if (currentChar == '\n') {
                                    currentChar = sourceReader.read();
                                }
                            } else {
                                if (currentChar == '\n') {

                                    currentChar = sourceReader.read();
                                }
                            }
                            if (currentChar < 0) {
                                break;
                            }
                            prevChar = currentChar;
                        }
                        continue;
                    } else if (currentChar == '/' && slashSlashComments) {
                        while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0) {
                            ;
                        }
                    }
                } else if (contextMarker == Character.MIN_VALUE && currentChar == '#' && hashComments) {

                    while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0) {
                        ;
                    }
                } else if (contextMarker == Character.MIN_VALUE && currentChar == '-' && dashDashComments) {
                    currentChar = sourceReader.read();

                    if (currentChar == -1 || currentChar != '-') {
                        buf.append('-');

                        if (currentChar != -1) {
                            buf.append(currentChar);
                        }

                        continue;
                    }



                    while ((currentChar = sourceReader.read()) != '\n' && currentChar != '\r' && currentChar >= 0) {
                        ;
                    }
                }

                if (currentChar != -1) {
                    buf.append((char) currentChar);
                }
            }
        } catch (IOException ioEx) {

        }

        return buf.toString();
    }

    public static String removeBetweenWithSplitorNotExistNull(String sql, String start, String end) {
        int index0 = sql.indexOf(start);
        if (index0 == -1) {
            return null;
        }
        int index1 = sql.indexOf(end, index0);
        if (index1 == -1) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(sql.substring(0, index0));
        sb.append(" ");
        sb.append(sql.substring(index1 + end.length()));
        return sb.toString();
    }


    public static boolean isTableFatherAndSon(String fatherTable, String sonTable) {
        if (fatherTable == null || fatherTable.trim().isEmpty() || sonTable == null || sonTable.trim().isEmpty()) {
            return false;
        }
        if (!sonTable.startsWith(fatherTable) || fatherTable.length() + 2 > sonTable.length()) {
            return false;
        }
        String suffix = sonTable.substring(fatherTable.length());
        if (suffix.matches("_[\\d]+")) {
            return true;
        }
        return false;

    }

    public static int compareTo(String str1, String str2) {
        if (str1 == null && str2 == null) {
            return 0;
        }

        if (str1 == null) {
            return -str2.compareTo(str1);
        }
        return str1.compareTo(str2);
    }

    public static String replaceWithIgnoreCase(String str, String searchStr, String replaceStr) {
        int index = indexOfIgnoreCase(str, searchStr);
        if (index < 0) {
            return str;
        } else {
            if (index > 0) {
                return str.substring(0, index) + replaceStr + str.substring(index + searchStr.length());
            } else {
                return replaceStr + str.substring(searchStr.length());
            }
        }
    }

    private static Method toPlainStringMethod;

    static {

        try {
            toPlainStringMethod = BigDecimal.class.getMethod("toPlainString", new Class[0]);
        } catch (NoSuchMethodException nsme) {

        }
    }

    public static final String fixDecimalExponent(String dString) {
        int ePos = dString.indexOf("E");

        if (ePos == -1) {
            ePos = dString.indexOf("e");
        }

        if (ePos != -1) {
            if (dString.length() > (ePos + 1)) {
                char maybeMinusChar = dString.charAt(ePos + 1);

                if (maybeMinusChar != '-' && maybeMinusChar != '+') {
                    StringBuffer buf = new StringBuffer(dString.length() + 1);
                    buf.append(dString.substring(0, ePos + 1));
                    buf.append('+');
                    buf.append(dString.substring(ePos + 1, dString.length()));
                    dString = buf.toString();
                }
            }
        }

        return dString;
    }

    public static String consistentToString(BigDecimal decimal) {
        if (decimal == null) {
            return null;
        }

        if (toPlainStringMethod != null) {
            try {
                return (String) toPlainStringMethod.invoke(decimal, (Object[]) null);
            } catch (InvocationTargetException invokeEx) {

            } catch (IllegalAccessException accessEx) {

            }
        }

        return decimal.toString();
    }

    public static String formatNanos(int nanos, boolean serverSupportsFracSecs) {
        if (!serverSupportsFracSecs || nanos == 0) {
            return "0";
        }

        boolean usingMicros = true;
        if (usingMicros) {
            nanos /= 1000;
        }

        final int digitCount = usingMicros ? 6 : 9;
        String nanosString = Integer.toString(nanos);
        final String zeroPadding = usingMicros ? "000000" : "000000000";

        nanosString = zeroPadding.substring(0, (digitCount - nanosString.length())) + nanosString;
        int pos = digitCount - 1;

        while (nanosString.charAt(pos) == '0') {
            pos--;
        }

        nanosString = nanosString.substring(0, pos + 1);
        return nanosString;
    }

    public static String placeHolder(int bit, long table) {
        if (bit > 18) {
            throw new IllegalArgumentException("截取的位数不能大于18位");
        }
        if (table == 0) {

            return String.valueOf(pow10[bit]).substring(1);
        }
        if (table >= pow10[bit - 1]) {

            return String.valueOf(table);
        }
        long max = pow10[bit];
        long placedNumber = max + table;
        return String.valueOf(placedNumber).substring(1);
    }

    public static boolean isEmpty(StringBuilder str) {
        return str == null || str.length() == 0;
    }

    public static boolean isEscapeNeededForString(String x, int stringLength) {
        boolean needsHexEscape = false;
        for (int i = 0; i < stringLength; ++i) {
            char c = x.charAt(i);
            switch (c) {
            case 0:
                needsHexEscape = true;
                break;
            case '\n':
                needsHexEscape = true;
                break;
            case '\r':
                needsHexEscape = true;
                break;
            case '\\':
                needsHexEscape = true;
                break;
            case '\'':
                needsHexEscape = true;
                break;
            case '"':
                needsHexEscape = true;
                break;
            case '\032':
                needsHexEscape = true;
                break;
            }

            if (needsHexEscape) {
                break;
            }
        }
        return needsHexEscape;
    }

    public static String backQuote(String x) {
        return "`" + x + "`";
    }

    public static String quoteString(String x) {
        String parameterAsString = null;
        boolean usingAnsiMode = false;
        int stringLength = x.length();
        StringBuffer buf = new StringBuffer((int) (x.length() * 1.1));
        buf.append('\'');
        if (isEscapeNeededForString(x, stringLength)) {

            for (int i = 0; i < stringLength; ++i) {
                char c = x.charAt(i);
                switch (c) {
                case 0:
                    buf.append('\\');
                    buf.append('0');
                    break;
                case '\n':
                    buf.append('\\');
                    buf.append('n');
                    break;
                case '\r':
                    buf.append('\\');
                    buf.append('r');
                    break;
                case '\\':
                    buf.append('\\');
                    buf.append('\\');
                    break;
                case '\'':
                    buf.append('\\');
                    buf.append('\'');
                    break;
                case '"':
                    if (usingAnsiMode) {
                        buf.append('\\');
                    }
                    buf.append('"');
                    break;
                case '\032':
                    buf.append('\\');
                    buf.append('Z');
                    break;
                case '\u00a5':
                case '\u20a9':
                    return null;
                default:
                    buf.append(c);
                }
            }
        } else {
            buf.append(x);
        }

        buf.append('\'');
        parameterAsString = buf.toString();
        return parameterAsString;
    }

    public static String bytesToHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            if ((0xff & b) < 0x10) {
                sb.append('0').append(Integer.toHexString((0xFF & b)));
            } else {
                sb.append(Integer.toHexString(0xFF & b));
            }
        }
        return sb.toString();
    }

    public static boolean containsIgnoreCase(String str, String searchStr) {
        if (str == null || searchStr == null) {
            return false;
        }
        int len = searchStr.length();
        if (len == 0) {
            return true;
        }
        char first = Character.toLowerCase(searchStr.charAt(0));
        int max = str.length() - len;
        for (int i = 0; i <= max; i++) {
            char ch = Character.toLowerCase(str.charAt(i));

            if (ch != first) {
                continue;
            }

            if (str.regionMatches(true, i + 1, searchStr, 1, len - 1)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsAny(String str, char[] searchChars) {
        if (isEmpty(str) || searchChars == null || searchChars.length == 0) {
            return false;
        }
        int csLength = str.length();
        int searchLength = searchChars.length;
        int csLast = csLength - 1;
        int searchLast = searchLength - 1;
        for (int i = 0; i < csLength; i++) {
            char ch = str.charAt(i);
            for (int j = 0; j < searchLength; j++) {
                if (searchChars[j] == ch) {
                    if (isHighSurrogate(ch)) {
                        if (j == searchLast) {

                            return true;
                        }
                        if (i < csLast && searchChars[j + 1] == str.charAt(i + 1)) {
                            return true;
                        }
                    } else {

                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean isHighSurrogate(char ch) {
        return ('\uD800' <= ch && '\uDBFF' >= ch);
    }

    public static String convertToHumpStr(String str) {
        StringBuilder sb = new StringBuilder();
        boolean match = false;
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (match && ch >= 97 && ch <= 122) {
                ch -= 32;
            }
            if (ch != '_') {
                match = false;
                sb.append(ch);
            } else {
                match = true;
            }
        }

        return sb.toString();
    }

    public static String convertToSnakeStr(String str) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (Character.getType(ch) == Character.UPPERCASE_LETTER) {
                sb.append("_" + Character.toLowerCase(ch));
            } else {
                sb.append(ch);
            }
        }

        return sb.toString();
    }

    public static boolean isParsableNumber(String str) {
        if (StringUtils.endsWith(str, ".")) {
            return false;
        }
        if (StringUtils.startsWith(str, "-")) {
            return NumberUtils.isDigits(StringUtils.replaceOnce(str.substring(1), ".", StringUtils.EMPTY));
        } else {
            return NumberUtils.isDigits(StringUtils.replaceOnce(str, ".", StringUtils.EMPTY));
        }
    }

    public static int lastDigitOf(String str) {
        int n = str.length() - 1;

        if (!Character.isDigit(str.charAt(n))) {
            return -1;
        }
        for (int i = n - 1; i >= 0; i--) {
            if (!Character.isDigit(str.charAt(i))) {
                return i + 1;
            }
        }
        return 0;
    }

    public static String escape(String str, char findChar, char leadChar) {
        int find = str.indexOf(findChar);
        if (find < 0) {
            return str;
        }
        StringBuilder builder = new StringBuilder(str.length() + 8);
        int index = 0;
        do {
            builder.append(str.substring(index, find));
            builder.append(leadChar);
            index = find;
            find = str.indexOf(findChar, find + 1);
        } while (find >= 0);

        builder.append(str.substring(index));
        return builder.toString();
    }

    public static String objToString(Object obj) {
        if (obj == null) {
            return null;
        }
        return obj.toString();
    }

    public static String normalizePriv(String str) {
        return StringUtils.isBlank(str) ? str : str.toUpperCase();
    }

    public static String addBacktick(String str) {
        if (StringUtils.isNotBlank(str) && !str.startsWith("`")) {
            str = "`" + str + "`";
        }
        return str;
    }

    public static String javaEncoding(String encoding) {
        if (encoding.equalsIgnoreCase("utf8mb4")) {
            return "utf8";
        } else if (encoding.equalsIgnoreCase("binary")) {
            return "iso_8859_1";
        }

        return encoding;
    }

    public static String getUnescapedString(String string) {
        return getUnescapedString(string, false);
    }

    public static String getUnescapedString(String string, boolean toUppercase) {
        StringBuilder sb = new StringBuilder();
        char[] chars = string.toCharArray();
        for (int i = 0; i < chars.length; ++i) {
            char c = chars[i];
            if (c == '\\') {
                switch (c = chars[++i]) {
                case '0':
                    sb.append('\0');
                    break;
                case 'b':
                    sb.append('\b');
                    break;
                case 'n':
                    sb.append('\n');
                    break;
                case 'r':
                    sb.append('\r');
                    break;
                case 't':
                    sb.append('\t');
                    break;
                case 'Z':
                    sb.append((char) 26);
                    break;
                case '\'':
                    sb.append('\'');
                    break;
                case '\\':
                    sb.append('\\');
                    break;
                case '\"':
                    sb.append("\"");
                    break;
                default:
                    sb.append(c);
                }
            } else if (c == '\'') {
                ++i;
                sb.append('\'');
            } else {
                if (toUppercase && c >= 'a' && c <= 'z') {
                    c -= 32;
                }
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public static String int2FixedLenHexStr(int intVal) {

        byte[] hashValBytes = intToByteArray(intVal);

        return bytesToHex(hashValBytes);
    }

    protected static String bytesToHex(byte[] bytes) {

        char[] buf = new char[bytes.length * 2];
        int a = 0;
        int index = 0;
        for (byte b : bytes) {
            if (b < 0) {
                a = 256 + b;
            } else {
                a = b;
            }
            buf[index++] = HEX_CHAR[a / 16];
            buf[index++] = HEX_CHAR[a % 16];
        }
        return new String(buf);
    }

    protected static byte[] intToByteArray(int i) {
        byte[] result = new byte[4];
        result[0] = (byte) ((i >> 24) & 0xFF);
        result[1] = (byte) ((i >> 16) & 0xFF);
        result[2] = (byte) ((i >> 8) & 0xFF);
        result[3] = (byte) (i & 0xFF);
        return result;
    }

    public static int hex2Int(String hexString) {
        int value = 0;
        for (int i = 0; i < hexString.length(); i++) {
            char ch = hexString.charAt(i);
            if (ch >= '0' && ch <= '9') {
                value <<= 4;
                value |= ch - '0';
                continue;
            }
            if (ch >= 'a' && ch <= 'f') {
                value <<= 4;
                value |= ch - 'a' + 10;
                continue;
            }
        }
        return value;
    }

    public static String[] truncate(String[] strings, int maxStringLength) {
        if (strings == null) {
            return null;
        }
        boolean needTruncate = false;
        for (String string : strings) {
            if (string != null && string.length() > maxStringLength) {
                needTruncate = true;
                break;
            }
        }

        if (!needTruncate) {
            return strings;
        }

        String[] strings1 = new String[strings.length];
        for (int i = 0; i < strings.length; i++) {
            if (strings[i] != null && strings[i].length() > maxStringLength) {
                strings1[i] = strings[i].substring(0, maxStringLength);
            } else {
                strings1[i] = strings[i];
            }

        }
        return strings1;
    }

    public static String concatTableName(String schema, String table) {
        return backQuote(schema) + "." + backQuote(table);
    }

}
