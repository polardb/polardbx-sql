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

package com.alibaba.polardbx.common.collation;

import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.common.charset.CharsetHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import org.yaml.snakeyaml.Yaml;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;

public abstract class AbstractUCACollationHandler extends AbstractCollationHandler {
    private static int[][] UCA_WEIGHTS;

    static {
        Yaml yaml = new Yaml();
        int[][] uca = yaml.loadAs(AbstractUCACollationHandler.class.getResourceAsStream("uca.yml"), int[][].class);

        int i = 0;
        int[] page000data = uca[i++];
        int[] page001data = uca[i++];
        int[] page002data = uca[i++];
        int[] page003data = uca[i++];
        int[] page004data = uca[i++];
        int[] page005data = uca[i++];
        int[] page006data = uca[i++];
        int[] page007data = uca[i++];
        int[] page009data = uca[i++];
        int[] page00Adata = uca[i++];
        int[] page00Bdata = uca[i++];
        int[] page00Cdata = uca[i++];
        int[] page00Ddata = uca[i++];
        int[] page00Edata = uca[i++];
        int[] page00Fdata = uca[i++];
        int[] page010data = uca[i++];
        int[] page011data = uca[i++];
        int[] page012data = uca[i++];
        int[] page013data = uca[i++];
        int[] page014data = uca[i++];
        int[] page015data = uca[i++];
        int[] page016data = uca[i++];
        int[] page017data = uca[i++];
        int[] page018data = uca[i++];
        int[] page019data = uca[i++];
        int[] page01Ddata = uca[i++];
        int[] page01Edata = uca[i++];
        int[] page01Fdata = uca[i++];
        int[] page020data = uca[i++];
        int[] page021data = uca[i++];
        int[] page022data = uca[i++];
        int[] page023data = uca[i++];
        int[] page024data = uca[i++];
        int[] page025data = uca[i++];
        int[] page026data = uca[i++];
        int[] page027data = uca[i++];
        int[] page028data = uca[i++];
        int[] page029data = uca[i++];
        int[] page02Adata = uca[i++];
        int[] page02Bdata = uca[i++];
        int[] page02Edata = uca[i++];
        int[] page02Fdata = uca[i++];
        int[] page030data = uca[i++];
        int[] page031data = uca[i++];
        int[] page032data = uca[i++];
        int[] page033data = uca[i++];
        int[] page04Ddata = uca[i++];
        int[] page0A0data = uca[i++];
        int[] page0A1data = uca[i++];
        int[] page0A2data = uca[i++];
        int[] page0A3data = uca[i++];
        int[] page0A4data = uca[i++];
        int[] page0F9data = uca[i++];
        int[] page0FAdata = uca[i++];
        int[] page0FBdata = uca[i++];
        int[] page0FCdata = uca[i++];
        int[] page0FDdata = uca[i++];
        int[] page0FEdata = uca[i++];
        int[] page0FFdata = uca[i++];

        UCA_WEIGHTS = new int[][] {
            page000data, page001data, page002data, page003data,
            page004data, page005data, page006data, page007data,
            null, page009data, page00Adata, page00Bdata,
            page00Cdata, page00Ddata, page00Edata, page00Fdata,
            page010data, page011data, page012data, page013data,
            page014data, page015data, page016data, page017data,
            page018data, page019data, null, null,
            null, page01Ddata, page01Edata, page01Fdata,
            page020data, page021data, page022data, page023data,
            page024data, page025data, page026data, page027data,
            page028data, page029data, page02Adata, page02Bdata,
            null, null, page02Edata, page02Fdata,
            page030data, page031data, page032data, page033data,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, page04Ddata, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            page0A0data, page0A1data, page0A2data, page0A3data,
            page0A4data, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, null, null, null,
            null, page0F9data, page0FAdata, page0FBdata,
            page0FCdata, page0FDdata, page0FEdata, page0FFdata
        };
    }

    private static int[] UCA_LENGTH = {
        4, 3, 3, 4, 3, 3, 3, 3, 0, 3, 3, 3, 3, 3, 3, 3,
        3, 3, 3, 3, 3, 2, 3, 3, 3, 3, 0, 0, 0, 3, 3, 3,
        5, 5, 4, 3, 5, 2, 3, 3, 2, 2, 5, 3, 0, 0, 3, 3,
        3, 3, 8, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        2, 2, 2, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 4, 3, 9, 3, 3
    };

    private static int MY_UCA_MAX_CONTRACTION = 6;
    private static int MAX_CHAR = 0xFFFF;
    private static boolean DIFF_IF_ONLY_ENDSPACE_DIFFERENCE = false;

    public AbstractUCACollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CharsetHandler getCharsetHandler() {
        return charsetHandler;
    }

    @Override
    public int compare(Slice str1, Slice str2) {
        UcaScanner scanner1 = new UcaScanner(str1);
        UcaScanner scanner2 = new UcaScanner(str2);
        int weight1, weight2;
        do {
            weight1 = scanner1.next();
            weight2 = scanner2.next();
        } while (weight1 == weight2 && weight1 > 0);
        return weight1 - weight2;
    }

    @Override
    public int compareSp(Slice str1, Slice str2) {
        UcaScanner scanner1 = new UcaScanner(str1);
        UcaScanner scanner2 = new UcaScanner(str2);
        int weight1, weight2;
        do {
            weight1 = scanner1.next();
            weight2 = scanner2.next();
        } while (weight1 == weight2 && weight1 > 0);

        if (weight1 > 0 && weight2 < 0) {
            weight2 = UCA_WEIGHTS[0][0x20 * UCA_LENGTH[0]];

            do {
                if (weight1 != weight2) {
                    return weight1 - weight2;
                }
                weight1 = scanner1.next();
            } while (weight1 > 0);
            return DIFF_IF_ONLY_ENDSPACE_DIFFERENCE ? 1 : 0;
        }

        if (weight1 < 0 && weight2 > 0) {
            weight1 = UCA_WEIGHTS[0][0x20 * UCA_LENGTH[0]];

            do {
                if (weight1 != weight2) {
                    return weight1 - weight2;
                }
                weight2 = scanner2.next();
            } while (weight2 > 0);
            return DIFF_IF_ONLY_ENDSPACE_DIFFERENCE ? -1 : 0;
        }

        return weight1 - weight2;
    }

    class UcaScanner {
        SliceInput inputString;
        IntBuffer weightString;
        int[] implicit;
        int page;
        int code;

        UcaScanner(Slice str) {
            this.inputString = str.getInput();
            this.weightString = noChar();
            this.implicit = new int[2];
            this.page = 0;
            this.code = 0;
        }

        IntBuffer noChar() {
            return IntBuffer.wrap(new int[] {0, 0});
        }

        int next() {
            if (weightString.get(weightString.position()) != 0) {
                return weightString.get();
            }
            do {
                int[] weightPage;
                int codepoint;

                if (!inputString.isReadable()) {
                    return -1;
                }

                codepoint = getCodePoint(inputString);
                if (codepoint == INVALID_CODE) {
                    return INVALID_CODE;
                }

                if (codepoint > MAX_CHAR) {
                    weightString = noChar();
                    return 0xFFFD;
                }

                page = codepoint >> 8;
                code = codepoint & 0xFF;

                weightPage = UCA_WEIGHTS[page];
                if (weightPage == null) {
                    code = (page << 8) + code;
                    implicit[0] = (code & 0x7FFF) | 0x8000;
                    implicit[1] = 0;
                    weightString = IntBuffer.wrap(Arrays.copyOf(implicit, 2));
                    page = page >> 7;
                    if (code >= 0x3400 && code <= 0x4DB5) {
                        page += 0xFB80;
                    } else if (code >= 0x4E00 && code <= 0x9FA5) {
                        page += 0xFB40;
                    } else {
                        page += 0xFBC0;
                    }
                    return page;
                }

                int index = code * UCA_LENGTH[page];
                weightString = IntBuffer.wrap(weightPage, index, weightPage.length - index);

            } while (weightString.get(weightString.position()) == 0);

            return weightString.get();
        }
    }

    @Override
    public SortKey getSortKey(Slice str, int maxLength) {
        ByteBuffer dst = ByteBuffer.allocate(maxLength);

        UcaScanner scanner = new UcaScanner(str);

        int weight;
        while (dst.hasRemaining() && (weight = scanner.next()) > 0) {
            dst.put((byte) (weight >> 8));
            if (dst.hasRemaining()) {
                dst.put((byte) (weight & 0xFF));
            }
        }

        int effectiveLen = dst.position();
        if (dst.hasRemaining()) {

            weight = UCA_WEIGHTS[0][0x20 * UCA_LENGTH[0]];

            while (dst.hasRemaining()) {
                dst.put((byte) (weight >> 8));
                if (dst.hasRemaining()) {
                    dst.put((byte) (weight & 0xFF));
                }
            }
        }

        return new SortKey(getCharsetName(), getName(), str.getInput(), dst.array(), effectiveLen);
    }

    @Override
    public int hashcode(Slice str) {
        long tmp1 = INIT_HASH_VALUE_1;
        long tmp2 = INIT_HASH_VALUE_2;

        int len = str.length();

        while (len >= 1 && str.getByte(len - 1) == 0x20) {
            len--;
        }
        str = str.slice(0, len);
        UcaScanner scanner = new UcaScanner(str);
        int weight;
        while ((weight = scanner.next()) > 0) {
            tmp1 ^= (((tmp1 & 0x3F) + tmp2) * (weight >> 8)) + (tmp1 << 8);
            tmp2 += 3;
            tmp1 ^= (((tmp1 & 0x3F) + tmp2) * (weight & 0xFF)) + (tmp1 << 8);
            tmp2 += 3;
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

        Slice utf8Str = Slices.wrappedBuffer(bytes, begin, end - begin);
        UcaScanner scanner = new UcaScanner(utf8Str);
        int weight;
        while ((weight = scanner.next()) > 0) {
            tmp1 ^= (((tmp1 & 0x3F) + tmp2) * (weight >> 8)) + (tmp1 << 8);
            tmp2 += 3;
            tmp1 ^= (((tmp1 & 0x3F) + tmp2) * (weight & 0xFF)) + (tmp1 << 8);
            tmp2 += 3;
        }

        numbers[0] = tmp1;
        numbers[1] = tmp2;
    }

    abstract int getCodePoint(SliceInput sliceInput);
}
