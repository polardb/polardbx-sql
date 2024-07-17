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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import org.yaml.snakeyaml.Yaml;

import java.nio.ByteBuffer;
import java.util.List;

public class Utf8mb4GeneralCiCollationHandler extends AbstractCollationHandler {

    public Utf8mb4GeneralCiCollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    public static List<int[][]> PAGES;
    public static int[][] EMPTY_PLANE = new int[][] {};
    public static final int MAX_CHAR = 0xFFFF;
    public static final int FAST_WEIGHT = 0xFFFD;

    static {
        Yaml yaml = new Yaml();
        int[][] plane00 =
            yaml.loadAs(Utf8mb4GeneralCiCollationHandler.class.getResourceAsStream("utf8_plane00.yml"), int[][].class);
        int[][] plane01 =
            yaml.loadAs(Utf8mb4GeneralCiCollationHandler.class.getResourceAsStream("utf8_plane01.yml"), int[][].class);
        int[][] plane02 =
            yaml.loadAs(Utf8mb4GeneralCiCollationHandler.class.getResourceAsStream("utf8_plane02.yml"), int[][].class);
        int[][] plane03 =
            yaml.loadAs(Utf8mb4GeneralCiCollationHandler.class.getResourceAsStream("utf8_plane03.yml"), int[][].class);
        int[][] plane04 =
            yaml.loadAs(Utf8mb4GeneralCiCollationHandler.class.getResourceAsStream("utf8_plane04.yml"), int[][].class);
        int[][] plane05 =
            yaml.loadAs(Utf8mb4GeneralCiCollationHandler.class.getResourceAsStream("utf8_plane05.yml"), int[][].class);
        int[][] plane1E =
            yaml.loadAs(Utf8mb4GeneralCiCollationHandler.class.getResourceAsStream("utf8_plane1E.yml"), int[][].class);
        int[][] plane1F =
            yaml.loadAs(Utf8mb4GeneralCiCollationHandler.class.getResourceAsStream("utf8_plane1F.yml"), int[][].class);
        int[][] plane21 =
            yaml.loadAs(Utf8mb4GeneralCiCollationHandler.class.getResourceAsStream("utf8_plane21.yml"), int[][].class);
        int[][] plane24 =
            yaml.loadAs(Utf8mb4GeneralCiCollationHandler.class.getResourceAsStream("utf8_plane24.yml"), int[][].class);
        int[][] planeFF =
            yaml.loadAs(Utf8mb4GeneralCiCollationHandler.class.getResourceAsStream("utf8_planeFF.yml"), int[][].class);

        PAGES = ImmutableList.of(
            plane00, plane01, plane02, plane03, plane04, plane05,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, plane1E,
            plane1F, EMPTY_PLANE, plane21, EMPTY_PLANE, EMPTY_PLANE,
            plane24, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE,
            EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, EMPTY_PLANE, planeFF
        );
    }

    @Override
    public CollationName getName() {
        return CollationName.UTF8MB4_GENERAL_CI;
    }

    @Override
    public boolean wildCompare(Slice slice, Slice wildCard) {
        return doWildCompare(slice, wildCard) == 0;
    }

    /**
     * FIXME high overhead implementation
     */
    @Override
    public boolean containsCompare(Slice slice, byte[] wildCard, int[] lps) {
        if (wildCard == null || wildCard.length == 0) {
            // like '%%' always match
            return true;
        }
        byte[] containWildCard = new byte[wildCard.length + 2];
        containWildCard[0] = WILD_MANY;
        System.arraycopy(wildCard, 0, containWildCard, 1, wildCard.length);
        containWildCard[containWildCard.length - 1] = WILD_MANY;
        Slice wildCardSlice = Slices.wrappedBuffer(containWildCard);
        return doWildCompare(slice, wildCardSlice) == 0;
    }

    public int doWildCompare(Slice slice, Slice wildCard) {
        int sliceCodePoint, wildCodePoint;
        SliceInput str = slice.getInput();
        SliceInput wildStr = wildCard.getInput();

        long[] strPositions = new long[2];
        long[] wildStrPositions = new long[2];

        while (wildStr.isReadable()) {
            while (true) {
                boolean escaped = false;

                wildCodePoint = codepointOfUTF8(wildStr, wildStrPositions);

                if (wildCodePoint == INVALID_CODE) {
                    return 1;
                }
                // found '%'
                if (wildCodePoint == WILD_MANY) {
                    // found an anchor char
                    break;
                }

                wildStr.setPosition(wildStrPositions[1]);

                // found '\'
                if (wildCodePoint == ESCAPE && wildStr.isReadable()) {
                    wildCodePoint = codepointOfUTF8(wildStr, wildStrPositions);
                    if (wildCodePoint == INVALID_CODE) {
                        return 1;
                    }
                    wildStr.setPosition(wildStrPositions[1]);
                    escaped = true;
                }

                sliceCodePoint = codepointOfUTF8(str, strPositions);
                if (sliceCodePoint == INVALID_CODE) {
                    return 1;
                }
                str.setPosition(strPositions[1]);

                // found '_'
                if (!escaped && wildCodePoint == WILD_ONE) {
                    // found an anchor char
                } else {
                    int sliceWeight = getWeight(sliceCodePoint);
                    int wildWeight = getWeight(wildCodePoint);
                    if (sliceWeight != wildWeight) {
                        // not matched
                        return 1;
                    }
                }

                if (!wildStr.isReadable()) {
                    // Match if both are at end
                    return str.isReadable() ? 1 : 0;
                }
            }

            if (wildCodePoint == WILD_MANY) {
                // Remove any '%' and '_' from the wild search string
                while (wildStr.isReadable()) {
                    wildCodePoint = codepointOfUTF8(wildStr, wildStrPositions);

                    if (wildCodePoint == INVALID_CODE) {
                        return 1;
                    }

                    if (wildCodePoint == WILD_MANY) {
                        wildStr.setPosition(wildStrPositions[1]);
                        continue;
                    }
                    if (wildCodePoint == WILD_ONE) {
                        wildStr.setPosition(wildStrPositions[1]);
                        sliceCodePoint = codepointOfUTF8(str, strPositions);
                        if (sliceCodePoint == INVALID_CODE) {
                            return 1;
                        }
                        str.setPosition(strPositions[1]);
                        continue;
                    }

                    // Not a wild character
                    break;
                }

                if (!wildStr.isReadable()) {
                    // ok if '%' is last
                    return 0;
                }

                if (!str.isReadable()) {
                    // not matched
                    return -1;
                }

                wildCodePoint = codepointOfUTF8(wildStr, wildStrPositions);
                if (wildCodePoint == INVALID_CODE) {
                    return 1;
                }
                wildStr.setPosition(wildStrPositions[1]);

                if (wildCodePoint == ESCAPE) {
                    if (wildStr.isReadable()) {
                        wildCodePoint = codepointOfUTF8(wildStr, wildStrPositions);
                        if (wildCodePoint == INVALID_CODE) {
                            return 1;
                        }
                        wildStr.setPosition(wildStrPositions[1]);
                    }
                }

                while (true) {
                    // skip until the first character from wildstr is found
                    while (str.isReadable()) {
                        sliceCodePoint = codepointOfUTF8(str, strPositions);

                        if (sliceCodePoint == INVALID_CODE) {
                            return 1;
                        }

                        int sliceWeight = getWeight(sliceCodePoint);
                        int wildWeight = getWeight(wildCodePoint);
                        if (sliceWeight == wildWeight) {
                            break;
                        }

                        str.setPosition(strPositions[1]);
                    }

                    if (!str.isReadable()) {
                        return -1;
                    }
                    str.setPosition(strPositions[1]);

                    int strPos = (int) str.position();
                    int wildPos = (int) wildStr.position();

                    int res = doWildCompare(
                        slice.slice(strPos, str.available()),
                        wildCard.slice(wildPos, wildStr.available())
                    );

                    str.setPosition(strPositions[1]);
                    wildStr.setPosition(wildStrPositions[1]);

                    if (res <= 0) {
                        return res;
                    }
                }
            }
        }
        return str.isReadable() ? 1 : 0;
    }

    @Override
    public SortKey getSortKey(Slice str, int maxLength) {
        SliceInput sliceInput = str.getInput();
        ByteBuffer dst = ByteBuffer.allocate(maxLength);

        while (dst.hasRemaining() && sliceInput.isReadable()) {
            int codepoint = codepointOfUTF8(sliceInput);
            if (codepoint == INVALID_CODE) {
                break;
            }


            int weight = getWeight(codepoint);
            dst.put((byte) (weight >> 8));
            if (dst.hasRemaining()) {
                dst.put((byte) (weight & 0xFF));
            }
        }

        int effectiveLen = dst.position();

        if (dst.hasRemaining()) {
            while (dst.remaining() >= 4) {
                dst.put((byte) 0x00);
                dst.put((byte) 0x20);
                dst.put((byte) 0x00);
                dst.put((byte) 0x20);
            }
            if (dst.remaining() >= 2) {
                dst.put((byte) 0x00);
                dst.put((byte) 0x20);
            }
            if (dst.hasRemaining()) {
                dst.put((byte) 0x00);
            }
        }

        return new SortKey(getCharsetName(), getName(), str.getInput(), dst.array(), effectiveLen);
    }

    @Override
    public int compareSp(Slice str1, Slice str2) {
        SliceInput leftInput = str1.getInput();
        SliceInput rightInput = str2.getInput();
        while (leftInput.isReadable() && rightInput.isReadable()) {
            long pos1 = leftInput.position();
            long pos2 = rightInput.position();

            int codepoint1 = codepointOfUTF8(leftInput);
            int codepoint2 = codepointOfUTF8(rightInput);

            if (codepoint1 == INVALID_CODE || codepoint2 == INVALID_CODE) {
                leftInput.setPosition(pos1);
                rightInput.setPosition(pos2);
                return binaryCompare(leftInput, rightInput);
            }

            int weight1 = getWeight(codepoint1);
            int weight2 = getWeight(codepoint2);

            if (weight1 != weight2) {
                return weight1 - weight2;
            }
        }
        int len1 = leftInput.available();
        int len2 = rightInput.available();
        int res = 0;

        if (len1 != len2) {
            int swap = 1;
            if (DIFF_IF_ONLY_END_SPACE_DIFFERENCE) {
                res = 1;
            }
            if (len1 < len2) {
                leftInput = rightInput;
                swap = -1;
                res = -res;
            }
            while (leftInput.isReadable()) {
                byte b = leftInput.readByte();
                if (Byte.toUnsignedInt(b) != ' ') {
                    return (Byte.toUnsignedInt(b) < ' ') ? -swap : swap;
                }
            }
        }
        return res;
    }

    @Override
    public int compare(Slice binaryStr1, Slice binaryStr2) {
        SliceInput leftInput = binaryStr1.getInput();
        SliceInput rightInput = binaryStr2.getInput();
        while (leftInput.isReadable() && rightInput.isReadable()) {
            long pos1 = leftInput.position();
            long pos2 = rightInput.position();

            int codepoint1 = codepointOfUTF8(leftInput);
            int codepoint2 = codepointOfUTF8(rightInput);

            if (codepoint1 == INVALID_CODE || codepoint2 == INVALID_CODE) {
                leftInput.setPosition(pos1);
                rightInput.setPosition(pos2);
                return binaryCompare(leftInput, rightInput);
            }

            int weight1 = getWeight(codepoint1);
            int weight2 = getWeight(codepoint2);

            if (weight1 != weight2) {
                return weight1 - weight2;
            }
        }
        return leftInput.available() - rightInput.available();
    }

    @Override
    public int compare(char[] utf16Char1, char[] utf16Char2) {
        return compare(new String(utf16Char1), new String(utf16Char2));
    }

    public static int getWeight(int codepoint) {
        Preconditions.checkArgument(codepoint <= 0x10ffff);
        if (codepoint > MAX_CHAR) {
            return FAST_WEIGHT;
        }
        int[][] matrix = PAGES.get(codepoint >> 8);
        if (matrix == EMPTY_PLANE) {
            return codepoint;
        }
        return matrix[codepoint & 0xff][2];
    }

    public static boolean isContinuationByte(byte c) {
        return (Byte.toUnsignedInt(c) ^ 0x80) < 0x40;
    }

    @Override
    public int hashcode(Slice utf8Str) {
        long tmp1 = INIT_HASH_VALUE_1;
        long tmp2 = INIT_HASH_VALUE_2;
        int ch;

        int len = utf8Str.length();
        while (len >= 1 && utf8Str.getByte(len - 1) == 0x20) {
            len--;
        }

        SliceInput sliceInput = utf8Str.slice(0, len).getInput();
        while (sliceInput.isReadable()) {
            int codepoint = codepointOfUTF8(sliceInput);
            if (codepoint == INVALID_CODE) {
                break;
            }

            int weight = getWeight(codepoint);

            ch = (weight & 0xFF);
            tmp1 ^= (((tmp1 & 63) + tmp2) * ch) + (tmp1 << 8);
            tmp2 += 3;

            ch = (weight >> 8) & 0xFF;
            tmp1 ^= (((tmp1 & 63) + tmp2) * ch) + (tmp1 << 8);
            tmp2 += 3;

            if (weight > 0xFFFF) {

                ch = (weight >> 16) & 0xFF;
                tmp1 ^= (((tmp1 & 63) + tmp2) * ch) + (tmp1 << 8);
                tmp2 += 3;
            }
        }

        return (int) tmp1;
    }

    @Override
    public void hashcode(byte[] bytes, int begin, int end, long[] numbers) {

        while (end >= begin + 1 && bytes[end - 1] == 0x20) {
            end--;
        }

        long tmp1 = numbers[0] & 0xffffffffL;
        long tmp2 = numbers[1] & 0xffffffffL;
        int[] results = new int[2];
        int ch, codepoint;
        while (begin < end) {
            codepointOfUTF8(bytes, begin, end, results);
            codepoint = results[0];
            begin = results[1];

            if (codepoint == INVALID_CODE) {
                break;
            }

            int weight = getWeight(codepoint);

            ch = (weight & 0xFF);
            tmp1 ^= (((tmp1 & 63) + tmp2) * ch) + (tmp1 << 8);
            tmp2 += 3;

            ch = (weight >> 8) & 0xFF;
            tmp1 ^= (((tmp1 & 63) + tmp2) * ch) + (tmp1 << 8);
            tmp2 += 3;

            if (weight > 0xFFFF) {

                ch = (weight >> 16) & 0xFF;
                tmp1 ^= (((tmp1 & 63) + tmp2) * ch) + (tmp1 << 8);
                tmp2 += 3;
            }
        }

        numbers[0] = tmp1;
        numbers[1] = tmp2;
    }

    @Override
    public int instr(Slice source, Slice target) {
        return instrForMultiBytes(source, target,
            sliceInput -> codepointOfUTF8(sliceInput));
    }
}
