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

package com.alibaba.polardbx.qatest.dql.sharding.charset;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class CharsetTestUtils {
    final static Random R = new Random();
    final static int EXCEPT_BYTE = 0x7F;
    final static int[] ONE_BYTE = {0x00, 0x7F};
    final static int[][] GBK_BYTE_LOW = {
        {0x00A1, 0x00A9},
        {0x00B0, 0x00F7},
        {0x0081, 0x00A0},
        {0x00AA, 0x00FE},
        {0x00A8, 0x00A9}
    };
    final static int[][] GBK_BYTE_HIGH = {
        {0x00A1, 0x00FE},
        {0x00A1, 0x00FE},
        {0x0040, 0x00FE},
        {0x0040, 0x00A0},
        {0x0040, 0x00A0}
    };
    final static int[] GBK_UNICODE_RANGE = {0x4E00, 0x9FA5};
    /* reference: https://openstd.samr.gov.cn/bzgk/gb/newGbInfo?hcno=C344D8D120B341A8DD328954A9B27A99 */
    final static int[] GB18030_UNICODE_RANGE = {0x0000, 0xFFFF};
    final static int[][][] GB18030_2_BYTES_RANGE = {
        {
            {0x81, 0xFE},
            {0x40, 0x7E},
        },
        {
            {0x81, 0xFE},
            {0x80, 0xFE},
        }
    };
    final static int[][] GB18030_4_BYTES_RANGE = {
        {0x81, 0xFE},
        {0x30, 0x39},
        {0x81, 0xFE},
        {0x30, 0x39}
    };
    final static int[] UTF16_UNICODE_RANGE = {0x0000, 0xFFFF};
    final static int[] UTF8MB4_UNICODE_RANGE = {0x0000, 0x10FFFF};
    final static int[] UTF8MB3_UNICODE_RANGE = {0x0000, 0xFFFF};
    final static int[] LATIN_UNICODE_RANGE = {0x0000, 0x0080};

    /**
     * Generate random utf8mb4 hex data.
     */
    public static List<byte[]> generateUTF16(int size, int maxChars, boolean trailingSpace) {
        return generateUnicode(UTF16_UNICODE_RANGE[0], UTF16_UNICODE_RANGE[1], size, maxChars, trailingSpace);
    }

    public static List<byte[]> generateUTF16Code(int size, int maxChars, boolean trailingSpace) {
        Random random = new Random();
        List<byte[]> bytesList = new ArrayList<>();
        ByteBuffer buff = ByteBuffer.allocate(4 * 5 + 10);
        for (int i = 0; i < size; i++) {
            // 1-5 characters for one string
            int characters = (Math.abs(random.nextInt()) % maxChars) + 1;
            buff.clear();
            for (int j = 0; j < characters; j++) {
                // we have 1/3 probability to get a supplementary character.
                int codepoint = random.nextInt() % 3 == 0 ? random.nextInt(0x10ffff) : random.nextInt(0x7f);
                byte[] bytes = codePointsToBytes(codepoint, "UTF-16");
                buff.put(bytes);
            }
            buff.flip();
            byte[] utf16str = new byte[buff.remaining()];
            buff.get(utf16str);
            bytesList.add(utf16str);

            // we have 1/3 probability to append trailing space
            while (trailingSpace && random.nextInt() % 3 == 0 && i < size) {
                int trailingSpaces = random.nextInt(5);
                byte[] newStr = new byte[utf16str.length + trailingSpaces * 2];
                // copy old str
                System.arraycopy(utf16str, 0, newStr, 0, utf16str.length);
                for (int j = utf16str.length; j < newStr.length; j += 2) {
                    newStr[j] = (byte) 0xFEFF;
                    newStr[j + 1] = (byte) 0x0020;
                }
                bytesList.add(newStr);
                i++;
            }
        }
        return bytesList;
    }

    /**
     * Generate random utf8mb4 hex data.
     */
    public static List<byte[]> generateUTF8MB4(int size, int maxChars, boolean trailingSpace) {
        List<byte[]> bytesList = new ArrayList<>();
        ByteBuffer buff = ByteBuffer.allocate(4 * 5 + 10);
        for (int i = 0; i < size; i++) {
            // 1-5 characters for one string
            int characters = (Math.abs(R.nextInt()) % maxChars) + 1;
            buff.clear();
            for (int j = 0; j < characters; j++) {
                // we have 1/3 probability to get a supplementary character.
                int codepoint = R.nextInt() % 3 == 0 ? R.nextInt(0x10ffff) : R.nextInt(0x7f);
                // ban space and invalid codepoint
                if (codepoint == 0x20 || (codepoint >= 0xd800 && codepoint <= 0xdfff)) {
                    j--;
                    continue;
                }
                byte[] bytes = codePointsToBytes(codepoint, "UTF-8");
                buff.put(bytes);
            }
            buff.flip();
            byte[] utf8str = new byte[buff.remaining()];
            buff.get(utf8str);
            bytesList.add(utf8str);

            // we have 1/3 probability to append trailing space
            while (trailingSpace && R.nextInt() % 3 == 0 && i < size) {
                int trailingSpaces = R.nextInt(5);
                byte[] newStr = new byte[utf8str.length + trailingSpaces];
                // fill with spaces
                Arrays.fill(newStr, (byte) 0x20);
                // copy old str
                System.arraycopy(utf8str, 0, newStr, 0, utf8str.length);
                bytesList.add(newStr);
                i++;
            }
        }
        return bytesList;
    }

    /**
     * Generate random utf8mb3 hex data.
     */
    public static List<byte[]> generateUTF8MB3(int size, int maxChars, boolean trailingSpace) {
        return generateUnicode(UTF8MB3_UNICODE_RANGE[0], UTF8MB3_UNICODE_RANGE[1], size, maxChars, trailingSpace);
    }

    /**
     * Generate random one-byte code.
     */
    public static List<byte[]> generateSimple(int size, int maxChars, boolean trailingSpace) {
        Random random = new Random();
        List<byte[]> bytesList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // 1-5 characters for one string
            int characters = random.nextInt(maxChars) + 1;

            // GBK use 2 bytes to represent a character.
            byte[] bytes = new byte[characters];
            for (int j = 0; j < characters; j++) {
                bytes[j] = (byte) random.nextInt(Byte.MAX_VALUE + 1);
            }
            bytesList.add(bytes);

            // we have 1/3 probability to append trailing space
            while (trailingSpace && random.nextInt() % 3 == 0 && i < size) {
                int trailingSpaces = random.nextInt(5);
                byte[] newStr = new byte[bytes.length + trailingSpaces];
                // fill with spaces
                Arrays.fill(newStr, (byte) 0x20);
                // copy old str
                System.arraycopy(bytes, 0, newStr, 0, bytes.length);
                bytesList.add(newStr);
                i++;
            }
        }
        return bytesList;
    }

    /**
     * Generate random byte code.
     */
    public static List<byte[]> generateBinary(int size) {
        Random random = new Random();
        List<byte[]> bytesList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // 1-5 characters for one string
            int characters = random.nextInt(5) + 1;

            // GBK use 2 bytes to represent a character.
            byte[] bytes = new byte[characters];
            for (int j = 0; j < characters; j++) {
                random.nextBytes(bytes);
            }
            bytesList.add(bytes);
        }
        return bytesList;
    }

    /**
     * Generate random gbk code.
     */
    public static List<byte[]> generateGBKCode(int size, int maxChars, boolean trailingSpace) {
        Random random = new Random();
        List<byte[]> bytesList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // 1-5 characters for one string
            int characters = (Math.abs(random.nextInt()) % maxChars) + 1;

            // GBK use 1 ~ 2 bytes to represent a character.
            ByteBuffer gbkBuffer = ByteBuffer.allocate(characters * 2);
            for (int j = 0; j < characters; j++) {
                int n = random.nextInt(6);
                if (n == 5) {
                    // one byte
                    int code = (int) (Math.random() * (ONE_BYTE[1] - ONE_BYTE[0]) + ONE_BYTE[0]);
                    gbkBuffer.put((byte) code);
                } else {
                    // two byte
                    int lowMin = GBK_BYTE_LOW[n][0];
                    int lowMax = GBK_BYTE_LOW[n][1];
                    int lowCode = (int) (Math.random() * (lowMax - lowMin) + lowMin);
                    Preconditions.checkArgument(lowMin <= lowCode && lowCode <= lowMax);

                    int highMin = GBK_BYTE_HIGH[n][0];
                    int highMax = GBK_BYTE_HIGH[n][1];
                    int highCode = (int) (Math.random() * (highMax - highMin) + highMin);
                    Preconditions.checkArgument(highMin <= highCode && highCode <= highMax);
                    if (highCode == EXCEPT_BYTE) {
                        continue;
                    }

                    gbkBuffer.put((byte) lowCode);
                    gbkBuffer.put((byte) highCode);
                }
            }

            gbkBuffer.flip();
            byte[] bytes = new byte[gbkBuffer.remaining()];
            gbkBuffer.get(bytes);
            bytesList.add(bytes);

            // we have 1/3 probability to append trailing space
            while (trailingSpace && random.nextInt() % 3 == 0 && i < size) {
                int trailingSpaces = random.nextInt(5);
                byte[] newStr = new byte[bytes.length + trailingSpaces];
                // fill with spaces
                Arrays.fill(newStr, (byte) 0x20);
                // copy old str
                System.arraycopy(bytes, 0, newStr, 0, bytes.length);
                bytesList.add(newStr);
                i++;
            }
        }
        return bytesList;
    }

    /**
     * Generate random gb18030 code.
     */
    public static List<byte[]> generateGB18030Code(int size, int maxChars, boolean trailingSpace) {
        Random random = new Random();
        List<byte[]> bytesList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // 1-8 characters for one string
            int characters = (Math.abs(random.nextInt()) % maxChars) + 1;

            // GB18030 use 1 ~ 4 bytes to represent a character.
            ByteBuffer gb18030Buffer = ByteBuffer.allocate(characters * 4);
            for (int j = 0; j < characters; j++) {
                int n = random.nextInt(4);
                if (n == 3) {
                    // 1 byte
                    int code = (int) (Math.random() * (ONE_BYTE[1] - ONE_BYTE[0]) + ONE_BYTE[0]);
                    gb18030Buffer.put((byte) code);
                } else if (n == 2) {
                    // 4 bytes
                    for (int k = 0; k < 4; k++) {
                        int codeMin = GB18030_4_BYTES_RANGE[k][0];
                        int codeMax = GB18030_4_BYTES_RANGE[k][1];
                        int code = (int) (Math.random() * (codeMax - codeMin) + codeMin);
                        Preconditions.checkArgument(codeMin <= code && code <= codeMax);
                        gb18030Buffer.put((byte) code);
                    }
                } else {
                    // 2 bytes
                    for (int k = 0; k < 2; k++) {
                        int codeMin = GB18030_2_BYTES_RANGE[n][k][0];
                        int codeMax = GB18030_2_BYTES_RANGE[n][k][1];
                        int code = (int) (Math.random() * (codeMax - codeMin) + codeMin);
                        Preconditions.checkArgument(codeMin <= code && code <= codeMax);
                        gb18030Buffer.put((byte) code);
                    }
                }
            }

            gb18030Buffer.flip();
            byte[] bytes = new byte[gb18030Buffer.remaining()];
            gb18030Buffer.get(bytes);
            bytesList.add(bytes);

            // we have 1/3 probability to append trailing space
            while (trailingSpace && random.nextInt() % 3 == 0 && i < size) {
                int trailingSpaces = random.nextInt(5);
                byte[] newStr = new byte[bytes.length + trailingSpaces];
                // fill with spaces
                Arrays.fill(newStr, (byte) 0x20);
                // copy old str
                System.arraycopy(bytes, 0, newStr, 0, bytes.length);
                bytesList.add(newStr);
                i++;
            }
        }
        return bytesList;
    }

    public static List<byte[]> generateGBKUnicode(int size, int maxChars, boolean trailingSpace) {
        return generateUnicode(GBK_UNICODE_RANGE[0], GBK_UNICODE_RANGE[1], size, maxChars, trailingSpace);
    }

    public static List<byte[]> generateGB18030Unicode(int size, int maxChars, boolean trailingSpace) {
        return generateUnicode(GB18030_UNICODE_RANGE[0], GB18030_UNICODE_RANGE[1], size, maxChars, trailingSpace);
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static byte[] codePointsToBytes(int codepoint, String charsetName) {
        Charset charset = Charset.forName(charsetName);
        return Optional.of(codepoint)
            .map(Character::toChars)
            .map(String::new)
            .map(s -> s.getBytes(charset))
            .orElseThrow(
                () -> new RuntimeException("invalid codepoint")
            );
    }

    public static String toLiteral(String charset, String collation, byte[] bytes) {
        String hex = bytesToHex(bytes);
        if (hex == null || hex.isEmpty()) {
            hex = "0000";
        }
        return "_" + charset + " " + "0x" + hex + " collate " + collation;
    }

    private static byte[] codePointToUtf8(int lowerBoundInclude, int upperBoundExclude) {
        Preconditions.checkArgument(lowerBoundInclude >= 0);
        Preconditions.checkArgument(upperBoundExclude > lowerBoundInclude);
        String str =
            new String(new char[] {(char) (R.nextInt(upperBoundExclude - lowerBoundInclude) + lowerBoundInclude)});
        return str == null ? null : str.getBytes();
    }

    private static List<byte[]> generateUnicode(int lowerInclude, int higherExclude, int size, int maxChars,
                                                boolean trailingSpace) {
        List<byte[]> bytesList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // 1-5 characters for one string
            int characters = (Math.abs(R.nextInt()) % maxChars) + 1;

            // utf8 use 1 ~ 4 bytes to represent a character.
            ByteBuffer gbkBuffer = ByteBuffer.allocate(characters * 4);
            for (int j = 0; j < characters; j++) {
                gbkBuffer.put(codePointToUtf8(lowerInclude, higherExclude));
            }

            gbkBuffer.flip();
            byte[] bytes = new byte[gbkBuffer.remaining()];
            gbkBuffer.get(bytes);
            bytesList.add(bytes);

            // we have 1/3 probability to append trailing space
            while (trailingSpace && R.nextInt() % 3 == 0 && i < size) {
                int trailingSpaces = R.nextInt(5);
                byte[] newStr = new byte[bytes.length + trailingSpaces];
                // fill with spaces
                Arrays.fill(newStr, (byte) 0x20);
                // copy old str
                System.arraycopy(bytes, 0, newStr, 0, bytes.length);
                bytesList.add(newStr);
                i++;
            }
        }
        return bytesList;
    }

}
