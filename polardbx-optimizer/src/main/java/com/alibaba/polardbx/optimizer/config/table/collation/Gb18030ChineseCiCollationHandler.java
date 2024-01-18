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

package com.alibaba.polardbx.optimizer.config.table.collation;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import org.yaml.snakeyaml.Yaml;

import java.nio.ByteBuffer;

public class Gb18030ChineseCiCollationHandler extends AbstractCollationHandler {
    public Gb18030ChineseCiCollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CollationName getName() {
        return CollationName.GB18030_CHINESE_CI;
    }

    @Override
    public CharsetName getCharsetName() {
        return CharsetName.GB18030;
    }

    /* The following three represent the min value of every single byte
    of 2/4-byte gb18030 code */
    final public static int MIN_MB_ODD_BYTE = 0x81;
    final public static int MIN_MB_EVEN_BYTE_2 = 0x40;
    final public static int MIN_MB_EVEN_BYTE_4 = 0x30;

    /* Difference between min gb18030 4-byte encoding 0x81308130 and
    max possible gb18030 encoding 0xFE39FE39 */
    final private static int MAX_GB18030_DIFF = 0x18398F;

    /* The difference used in mapping 2-byte unicode to 4-byte gb18030 */
    final private static int UNI2_TO_GB4_DIFF = 7456;

    /* The offset used in unicase mapping is for those 4-byte gb18030,
    which are mapped by the diff, plus the offset */
    final private static int UNICASE_4_BYTE_OFFSET = 0x80;

    /* The following two used in unicase mapping are for 2-byte gb18030,
    which are mapped directly */
    final private static int MIN_2_BYTE_UNICASE = 0xA000;
    final private static int MAX_2_BYTE_UNICASE = 0xDFFF;

    /* The following two used in unicase mapping are for 3-byte unicode,
    and they are mapped to 4-byte gb18030 */
    final private static int MIN_3_BYTE_FROM_UNI = 0x2E600;
    final private static int MAX_3_BYTE_FROM_UNI = 0x2E6FF;

    /* 3 ranges(included) that cover the Chinese characters defined by the
    collation PINYIN in UNICODE CLDR24 */
    final private static int PINYIN_2_BYTE_START = 0x8140;
    final private static int PINYIN_2_BYTE_END = 0xFE9F;

    final private static int PINYIN_4_BYTE_1_START = 0x8138FD38;
    final private static int PINYIN_4_BYTE_1_END = 0x82359232;
    final private static int PINYIN_4_1_DIFF = 11328;

    final private static int PINYIN_4_BYTE_2_START = 0x95328236;
    final private static int PINYIN_4_BYTE_2_END = 0x98399836;
    final private static int PINYIN_4_2_DIFF = 254536;

    /* Two base used for weight, PINYIN is for Chinese chars and COMMON
    is for all other 4-byte non-Chinese chars */
    final private static int PINYIN_WEIGHT_BASE = 0xFFA00000;
    final private static int COMMON_WEIGHT_BASE = 0xFF000000;

    final private static int[] SORT_ORDER_GB18030 = {
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B,
        0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
        0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, ' ', '!', '"', '#',
        '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', ':', ';',
        '<', '=', '>', '?', '@', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
        'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
        'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '[', '\\', ']', '^', '_',
        '`', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
        'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
        'X', 'Y', 'Z', '{', '|', '}', '~', 0x7F, 0x80, 0x81, 0x82, 0x83,
        0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F,
        0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B,
        0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7,
        0xA8, 0xA9, 0xAA, 0xAB, 0xAC, 0xAD, 0xAE, 0xAF, 0xB0, 0xB1, 0xB2, 0xB3,
        0xB4, 0xB5, 0xB6, 0xB7, 0xB8, 0xB9, 0xBA, 0xBB, 0xBC, 0xBD, 0xBE, 0xBF,
        0xC0, 0xC1, 0xC2, 0xC3, 0xC4, 0xC5, 0xC6, 0xC7, 0xC8, 0xC9, 0xCA, 0xCB,
        0xCC, 0xCD, 0xCE, 0xCF, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7,
        0xD8, 0xD9, 0xDA, 0xDB, 0xDC, 0xDD, 0xDE, 0xDF, 0xE0, 0xE1, 0xE2, 0xE3,
        0xE4, 0xE5, 0xE6, 0xE7, 0xE8, 0xE9, 0xEA, 0xEB, 0xEC, 0xED, 0xEE, 0xEF,
        0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB,
        0xFC, 0xFD, 0xFE, 0xFF
    };

    private static final int[] GB18030_2_WEIGHT_PY;
    private static final int[] GB18030_4_WEIGHT_PY_P1;
    private static final int[] GB18030_4_WEIGHT_PY_P2;

    /* [toupper, tolower, sort] */
    private static final int[][][] CASEINFO_PAGES_GB18030;

    private static final int[][] PLANE00;
    private static final int[][] PLANE01;
    private static final int[][] PLANE02;
    private static final int[][] PLANE03;
    private static final int[][] PLANE04;
    private static final int[][] PLANE10;
    private static final int[][] PLANE1D;
    private static final int[][] PLANE1E;
    private static final int[][] PLANE1F;
    private static final int[][] PLANE20;
    private static final int[][] PLANE23;
    private static final int[][] PLANE2A;
    private static final int[][] PLANE2B;
    private static final int[][] PLANE51;
    private static final int[][] PLANE52;
    private static final int[][] PLANEA2;
    private static final int[][] PLANEA3;
    private static final int[][] PLANEA6;
    private static final int[][] PLANEA7;
    private static final int[][] PLANEA8;
    private static final int[][] PLANEE6;

    static {
        Yaml yaml = new Yaml();
        GB18030_2_WEIGHT_PY =
            yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_2_weight_py.yml"),
                int[].class);
        GB18030_4_WEIGHT_PY_P1 =
            yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_4_weight_py_p1.yml"),
                int[].class);
        GB18030_4_WEIGHT_PY_P2 =
            yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_4_weight_py_p2.yml"),
                int[].class);
        PLANE00 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane00.yml"),
            int[][].class);
        PLANE01 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane01.yml"),
            int[][].class);
        PLANE02 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane02.yml"),
            int[][].class);
        PLANE03 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane03.yml"),
            int[][].class);
        PLANE04 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane04.yml"),
            int[][].class);
        PLANE10 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane10.yml"),
            int[][].class);
        PLANE1D = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane1D.yml"),
            int[][].class);
        PLANE1E = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane1E.yml"),
            int[][].class);
        PLANE1F = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane1F.yml"),
            int[][].class);
        PLANE20 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane20.yml"),
            int[][].class);
        PLANE23 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane23.yml"),
            int[][].class);
        PLANE2A = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane2A.yml"),
            int[][].class);
        PLANE2B = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane2B.yml"),
            int[][].class);
        PLANE51 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane51.yml"),
            int[][].class);
        PLANE52 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_plane52.yml"),
            int[][].class);
        PLANEA2 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_planeA2.yml"),
            int[][].class);
        PLANEA3 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_planeA3.yml"),
            int[][].class);
        PLANEA6 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_planeA6.yml"),
            int[][].class);
        PLANEA7 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_planeA7.yml"),
            int[][].class);
        PLANEA8 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_planeA8.yml"),
            int[][].class);
        PLANEE6 = yaml.loadAs(Gb18030ChineseCiCollationHandler.class.getResourceAsStream("gb18030_planeE6.yml"),
            int[][].class);
        CASEINFO_PAGES_GB18030 = new int[][][] {
            PLANE00, PLANE01, PLANE02, PLANE03, PLANE04, null, null, null,
            null, null, null, null, null, null, null, null,
            PLANE10, null, null, null, null, null, null, null,
            null, null, null, null, null, PLANE1D, PLANE1E, PLANE1F,
            PLANE20, null, null, PLANE23, null, null, null, null,
            null, null, PLANE2A, PLANE2B, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, PLANE51, PLANE52, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, PLANEA2, PLANEA3, null, null, PLANEA6, PLANEA7,
            PLANEA8, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, PLANEE6, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null
        };
    }

    @Override
    public int compare(Slice str1, Slice str2) {
        int len1 = str1.length();
        int len2 = str2.length();
        int pos1 = 0, pos2 = 0;

        while (pos1 < len1 && pos2 < len2) {
            int mbLen1 = getMbLength(str1.slice(pos1, len1 - pos1));
            int mbLen2 = getMbLength(str2.slice(pos2, len2 - pos2));

            if (mbLen1 > 0 && mbLen2 > 0) {
                int code1 = getGB18030Code(str1.slice(pos1, mbLen1));
                int code2 = getGB18030Code(str2.slice(pos2, mbLen2));

                if (code1 != code2) {
                    return code1 > code2 ? 1 : -1;
                }

                pos1 += mbLen1;
                pos2 += mbLen2;
            } else if (mbLen1 == 0 && mbLen2 == 0) {
                int sortOrder1 = SORT_ORDER_GB18030[Byte.toUnsignedInt(str1.getByte(pos1++))];
                int sortOrder2 = SORT_ORDER_GB18030[Byte.toUnsignedInt(str2.getByte(pos2++))];

                if (sortOrder1 != sortOrder2) {
                    return sortOrder1 - sortOrder2;
                }
            } else {
                return mbLen1 == 0 ? -1 : 1;
            }
        }

        return len1 - len2;
    }

    @Override
    public SortKey getSortKey(Slice str, int maxLength) {
        ByteBuffer dst = ByteBuffer.allocate(maxLength);

        int len = str.length();
        int pos = 0;
        while (pos < len && dst.hasRemaining()) {
            int mbLen = getMbLength(str.slice(pos, len - pos));
            int code;

            if (mbLen > 0) {
                code = getGB18030Code(str.slice(pos, mbLen));

                pos += mbLen;
            } else {
                code = SORT_ORDER_GB18030[Byte.toUnsignedInt(str.getByte(pos++))];
            }

            dst.put((byte) (code >> 24));

            if (dst.hasRemaining()) {
                dst.put((byte) ((code >> 16) & 0xFF));
            }

            if (dst.hasRemaining()) {
                dst.put((byte) ((code >> 8) & 0xFF));
            }

            if (dst.hasRemaining()) {
                dst.put((byte) (code & 0xFF));
            }
        }

        int effectiveLen = dst.position();

        if (dst.hasRemaining()) {
            while (dst.remaining() >= 4) {
                dst.put((byte) 0x00);
                dst.put((byte) 0x00);
                dst.put((byte) 0x00);
                dst.put((byte) 0x20);
            }

            while (dst.hasRemaining()) {
                dst.put((byte) 0x00);
            }
        }

        return new SortKey(getCharsetName(), getName(), str.getInput(), dst.array(), effectiveLen);
    }

    @Override
    public int hashcode(Slice str) {
        long tmp1 = INIT_HASH_VALUE_1;
        long tmp2 = INIT_HASH_VALUE_2;
        // skip trailing space
        int len = str.length();
        while (len >= 1 && str.getByte(len - 1) == 0x20) {
            len--;
        }

        int pos = 0;
        while (pos < len) {
            int mbLen = getMbLength(str.slice(pos, len - pos));
            int code;

            if (mbLen > 0) {
                code = getGB18030Code(str.slice(pos, mbLen));

                pos += mbLen;
            } else {
                code = SORT_ORDER_GB18030[Byte.toUnsignedInt(str.getByte(pos++))];
            }

            for (int i = 0; i < 4; i++) {
                tmp1 ^= (((tmp1 & 63) + tmp2) * ((code >> (i * 8)) & 0xFF)) + (tmp1 << 8);
                tmp2 += 3;
            }
        }

        return (int) tmp1;
    }

    @Override
    public void hashcode(byte[] bytes, int begin, int end, long[] numbers) {
        long tmp1 = Integer.toUnsignedLong((int) numbers[0]);
        long tmp2 = Integer.toUnsignedLong((int) numbers[1]);

        // skip trailing space
        while (end >= begin + 1 && bytes[end - 1] == 0x20) {
            end--;
        }

        for (int pos = begin; pos < end; ) {
            int mbLen = getMbLength(Slices.wrappedBuffer(bytes, pos, end - pos));
            int code;

            if (mbLen > 0) {
                code = getGB18030Code(Slices.wrappedBuffer(bytes, pos, mbLen));

                pos += mbLen;
            } else {
                code = SORT_ORDER_GB18030[Byte.toUnsignedInt(bytes[pos++])];
            }

            for (int i = 0; i < 4; i++) {
                tmp1 ^= (((tmp1 & 63) + tmp2) * ((code >> (i * 8)) & 0xFF)) + (tmp1 << 8);
                tmp2 += 3;
            }
        }

        numbers[0] = tmp1;
        numbers[1] = tmp2;
    }

    @Override
    public int compareSp(Slice str1, Slice str2) {
        int len1 = str1.length();
        int len2 = str2.length();
        int pos1 = 0, pos2 = 0;

        while (pos1 < len1 && pos2 < len2) {
            int mbLen1 = getMbLength(str1.slice(pos1, len1 - pos1));
            int mbLen2 = getMbLength(str2.slice(pos2, len2 - pos2));

            if (mbLen1 > 0 && mbLen2 > 0) {
                int code1 = getGB18030Code(str1.slice(pos1, mbLen1));
                int code2 = getGB18030Code(str2.slice(pos2, mbLen2));

                if (code1 != code2) {
                    return code1 > code2 ? 1 : -1;
                }

                pos1 += mbLen1;
                pos2 += mbLen2;
            } else if (mbLen1 == 0 && mbLen2 == 0) {
                int sortOrder1 = SORT_ORDER_GB18030[Byte.toUnsignedInt(str1.getByte(pos1++))];
                int sortOrder2 = SORT_ORDER_GB18030[Byte.toUnsignedInt(str2.getByte(pos2++))];

                if (sortOrder1 != sortOrder2) {
                    return sortOrder1 - sortOrder2;
                }
            } else {
                return mbLen1 == 0 ? -1 : 1;
            }
        }

        if (pos1 < len1 || pos2 < len2) {
            int swap = 1;
            if (len1 < len2) {
                str1 = str2;
                pos1 = pos2;
                len1 = len2;
                swap = -1;
            }
            for (; pos1 < len1; pos1++) {
                if (Byte.toUnsignedInt(str1.getByte(pos1)) != 0x20) {
                    return Byte.toUnsignedInt(str1.getByte(pos1)) < 0x20 ? -swap : swap;
                }
            }
        }

        return 0;
    }

    @Override
    public int instr(Slice source, Slice target) {
        return instrForMultiBytes(source, target, Gb18030ChineseCiCollationHandler::codeOfGB18030);
    }

    public static int codeOfGB18030(SliceInput sliceInput) {
        if (sliceInput.available() <= 0) {
            return INVALID_CODE;
        }

        byte b1 = sliceInput.readByte();
        if (sliceInput.isReadable() && isMbOdd(b1)) {
            byte b2 = sliceInput.readByte();
            if (isMbEven2(b2)) {
                // 2 bytes code.
                return getGB18030Code(Slices.wrappedBuffer(b1, b2));
            } else {
                if (sliceInput.available() >= 2) {
                    byte b3 = sliceInput.readByte();
                    byte b4 = sliceInput.readByte();
                    if (isMbEven4(b2) && isMbOdd(b3) && isMbEven4(b4)) {
                        // 4 bytes code.
                        return getGB18030Code(Slices.wrappedBuffer(b1, b2, b3, b4));
                    } else {
                        sliceInput.setPosition(sliceInput.position() - 3);
                    }
                } else {
                    sliceInput.setPosition(sliceInput.position() - 1);
                }
            }
        }

        // 1 byte code.
        return SORT_ORDER_GB18030[Byte.toUnsignedInt(b1)];
    }

    public static int getGB18030ChsToCode(Slice str) {
        int r = 0;
        int len = str.length();
        assert (len == 1 || len == 2 || len == 4);

        switch (len) {
        case 1:
            r = Byte.toUnsignedInt(str.getByte(0));
            break;
        case 2:
            r = (Byte.toUnsignedInt(str.getByte(0)) << 8) + Byte.toUnsignedInt(str.getByte(1));
            break;
        case 4:
            r = (Byte.toUnsignedInt(str.getByte(0)) << 24) + (Byte.toUnsignedInt(str.getByte(1)) << 16) + (
                Byte.toUnsignedInt(str.getByte(2)) << 8) + Byte.toUnsignedInt(str.getByte(3));
            break;
        default:
            assert (false);
        }
        return r;
    }

    private static int getWeightIfChineseCharacter(int code) {
        if (Integer.compareUnsigned(code, PINYIN_2_BYTE_START) >= 0
            && Integer.compareUnsigned(code, PINYIN_2_BYTE_END) <= 0) {
            int idx = (((code >> 8) & 0xFF) - MIN_MB_ODD_BYTE) * 0xBE + (code & 0xFF) -
                MIN_MB_EVEN_BYTE_2;
            if ((code & 0xFF) > 0x7F) {
                idx -= 0x01;
            }

            return PINYIN_WEIGHT_BASE + GB18030_2_WEIGHT_PY[idx];
        } else if (Integer.compareUnsigned(code, PINYIN_4_BYTE_1_START) >= 0
            && Integer.compareUnsigned(code, PINYIN_4_BYTE_1_END) <= 0) {
            int idx = getGB180304CodeToDiff(code) - PINYIN_4_1_DIFF;
            return PINYIN_WEIGHT_BASE + GB18030_4_WEIGHT_PY_P1[idx];
        } else if (Integer.compareUnsigned(code, PINYIN_4_BYTE_2_START) >= 0
            && Integer.compareUnsigned(code, PINYIN_4_BYTE_2_END) <= 0) {
            int idx = getGB180304CodeToDiff(code) - PINYIN_4_2_DIFF;
            return PINYIN_WEIGHT_BASE + GB18030_4_WEIGHT_PY_P2[idx];
        }

        return PINYIN_WEIGHT_BASE;
    }

    public static int getGB180304ChsToDiff(Slice str) {
        return (Byte.toUnsignedInt(str.getByte(0)) - MIN_MB_ODD_BYTE) * 12600 +
            (Byte.toUnsignedInt(str.getByte(1)) - MIN_MB_EVEN_BYTE_4) * 1260 +
            (Byte.toUnsignedInt(str.getByte(2)) - MIN_MB_ODD_BYTE) * 10 +
            (Byte.toUnsignedInt(str.getByte(3)) - MIN_MB_EVEN_BYTE_4);
    }

    private static int[] getCaseInfo(Slice str) {
        int len = str.length();
        int diff, code;
        int[][] p;

        switch (len) {
        case 1:
            return CASEINFO_PAGES_GB18030[0][Byte.toUnsignedInt(str.getByte(0))];
        case 2:
            if (Byte.toUnsignedInt(str.getByte(0)) < ((MIN_2_BYTE_UNICASE >> 8) & 0xFF) ||
                Byte.toUnsignedInt(str.getByte(0)) > ((MAX_2_BYTE_UNICASE >> 8) & 0xFF)) {
                return null;
            }
            p = CASEINFO_PAGES_GB18030[Byte.toUnsignedInt(str.getByte(0))];
            return p != null ? p[Byte.toUnsignedInt(str.getByte(1))] : null;
        case 4:
            diff = getGB180304ChsToDiff(str);
            code = 0;

            if (diff < MIN_2_BYTE_UNICASE - UNICASE_4_BYTE_OFFSET) {
                code = diff + UNICASE_4_BYTE_OFFSET;
            } else if (diff >= MIN_3_BYTE_FROM_UNI && diff <= MAX_3_BYTE_FROM_UNI) {
                code = (diff & 0xFFFF);
            } else {
                return null;
            }

            p = CASEINFO_PAGES_GB18030[(code >> 8) & 0xFF];
            return p != null ? p[code & 0xFF] : null;
        default:
            assert (false);
        }
        return null;
    }

    private static Slice diffToGB180304(int length, int diff) {
        byte[] dst = new byte[4];
        assert (length >= 4);

        if (diff > MAX_GB18030_DIFF) {
            return new Slice();
        }
        dst[3] = (byte) ((diff % 10) + MIN_MB_EVEN_BYTE_4);
        diff /= 10;
        dst[2] = (byte) ((diff % 126) + MIN_MB_ODD_BYTE);
        diff /= 126;
        dst[1] = (byte) ((diff % 10) + MIN_MB_EVEN_BYTE_4);
        dst[0] = (byte) ((diff / 10) + MIN_MB_ODD_BYTE);

        return Slices.wrappedBuffer(dst, 0, 4);
    }

    private static int getCaseInfoCodeToGB18030(int code) {
        if ((code >= MIN_2_BYTE_UNICASE && code <= MAX_2_BYTE_UNICASE) ||
            code < UNICASE_4_BYTE_OFFSET) {
            return code;
        } else {
            if (code < MIN_2_BYTE_UNICASE) {
                code -= UNICASE_4_BYTE_OFFSET;
            } else if (code >= (MIN_3_BYTE_FROM_UNI & 0xFFFF) &&
                code <= (MAX_3_BYTE_FROM_UNI & 0xFFFF)) {
                code += (MIN_3_BYTE_FROM_UNI & 0xFF0000);
            } else {
                assert (false);
            }

            Slice gbchs = diffToGB180304(4, code);
            assert (gbchs.length() == 4);

            return getGB18030ChsToCode(gbchs);
        }
    }

    private static int getCaseFoldedCode(Slice str, boolean isUpper) {
        int[] ch = getCaseInfo(str);
        int len = str.length();

        assert (len == 1 || len == 2 || len == 4);

        return ch != null ? getCaseInfoCodeToGB18030(isUpper ? ch[0] : ch[1]) : 0;
    }

    private static int getGB180304CodeToDiff(int code) {
        int diff = 0;

        assert (isMbOdd((byte) ((code >> 24) & 0xFF)));
        diff += ((code >> 24) & 0xFF) - MIN_MB_ODD_BYTE;
        diff *= 10;
        assert (isMbEven4((byte) ((code >> 16) & 0xFF)));
        diff += ((code >> 16) & 0xFF) - MIN_MB_EVEN_BYTE_4;
        diff *= 126;
        assert (isMbOdd((byte) ((code >> 8) & 0xFF)));
        diff += ((code >> 8) & 0xFF) - MIN_MB_ODD_BYTE;
        diff *= 10;
        assert (isMbEven4((byte) (code & 0xFF)));
        diff += (code & 0xFF) - MIN_MB_EVEN_BYTE_4;

        return diff;
    }

    public static int getGB18030Code(Slice str) {
        int weight, caseUpCode;
        int code = getGB18030ChsToCode(str);
        int mbLen = str.length();

        assert (mbLen == 2 || mbLen == 4);

        /* Make sure the max 4-byte gb18030 code has the max weight */
        if (code == 0xFE39FE39) {
            return 0xFFFFFFFF;
        }

        weight = getWeightIfChineseCharacter(code);
        if (Integer.compareUnsigned(weight, PINYIN_WEIGHT_BASE) > 0) {
            return weight;
        }

        caseUpCode = getCaseFoldedCode(str, true);
        if (caseUpCode == 0) {
            caseUpCode = code;
        }

        weight = (caseUpCode <= 0xFFFF) ? caseUpCode : COMMON_WEIGHT_BASE + getGB180304CodeToDiff(caseUpCode);

        return weight;
    }

    public static int getMbLength(Slice str) {
        assert (str.length() > 0);

        if (str.length() <= 1 || !isMbOdd(str.getByte(0))) {
            return 0;
        }

        if (isMbEven2(str.getByte(1))) {
            return 2;
        } else if (str.length() > 3 && isMbEven4(str.getByte(1)) && isMbOdd(str.getByte(2)) &&
            isMbEven4(str.getByte(3))) {
            return 4;
        }

        return 0;
    }

    public static boolean isMb1(byte b) {
        return Byte.toUnsignedInt(b) <= 0x7F;
    }

    public static boolean isMbOdd(byte b) {
        int x = Byte.toUnsignedInt(b);
        return x >= 0x81 && x <= 0xFE;
    }

    public static boolean isMbEven2(byte b) {
        int x = Byte.toUnsignedInt(b);
        return (x >= 0x40 && x <= 0x7E) ||
            (x >= 0x80 && x <= 0xFE);
    }

    public static boolean isMbEven4(byte b) {
        int x = Byte.toUnsignedInt(b);
        return x >= 0x30 && x <= 0x39;
    }
}
