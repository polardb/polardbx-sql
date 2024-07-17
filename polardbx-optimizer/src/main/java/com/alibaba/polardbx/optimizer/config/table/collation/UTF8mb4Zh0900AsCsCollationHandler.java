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

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.optimizer.config.table.collation.CTypeUCA.my_charset_utf8mb4_zh_0900_as_cs;
import static com.alibaba.polardbx.optimizer.config.table.collation.CTypeUCA.zh_coll_param;
import static com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.*;
import static com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.MY_UCA_PREVIOUS_CONTEXT_HEAD;
import static com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.MY_UCA_PREVIOUS_CONTEXT_TAIL;
import static com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.enum_case_first.CASE_FIRST_UPPER;
import static com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.enum_uca_ver.UCA_V900;
import static com.alibaba.polardbx.optimizer.config.table.collation.Uca900Data.MY_UCA_900_CE_SIZE;
import static com.alibaba.polardbx.optimizer.config.table.collation.Uca900Data.UCA900_DISTANCE_BETWEEN_WEIGHTS;
import static com.alibaba.polardbx.optimizer.config.table.collation.Uca900Data.UCA900_WEIGHT_ADDR;

public class UTF8mb4Zh0900AsCsCollationHandler extends AbstractCollationHandler {

    public UTF8mb4Zh0900AsCsCollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    @Override
    public CollationName getName() {
        return CollationName.UTF8MB4_ZH_0900_AS_CS;
    }

    @Override
    public CharsetHandler getCharsetHandler() {
        return charsetHandler;
    }

    @Override
    public int compare(Slice str1, Slice str2) {
        MCType.CHARSET_INFO cs = my_charset_utf8mb4_zh_0900_as_cs;
        boolean t_is_prefix = false;

        ZhUcaScanner sscanner = new ZhUcaScanner(str1);
        ZhUcaScanner tscanner = new ZhUcaScanner(str2);
        int s_res = 0;
        int t_res = 0;

        // We compare 2 strings in same level first. If only string A's scanner
        // has gone to next level, which means another string, B's weight of
        // current level is longer than A's. We'll compare B's remaining weights
        // with space.
        for (int current_lv = 0; current_lv < cs.levels_for_compare; ++current_lv) {
            /* Run the scanners until one of them runs out of current lv */
            do {
                s_res = sscanner.next();
                t_res = tscanner.next();
            } while (s_res == t_res && s_res >= 0 &&
                sscanner.get_weight_level() == current_lv &&
                tscanner.get_weight_level() == current_lv);

            // Two scanners run to next level at same time, or we found a difference,
            // or we found an error.
            if (sscanner.get_weight_level() == tscanner.get_weight_level()) {
                if (s_res == t_res && s_res >= 0) {
                    continue;
                }
                break;  // Error or inequality found, end.
            }

            if (tscanner.get_weight_level() > current_lv) {
                // t ran out of weights on this level, and s didn't.
                if (t_is_prefix) {
                    // Consume the rest of the weights from s.
                    do {
                        s_res = sscanner.next();
                    } while (s_res >= 0 && sscanner.get_weight_level() == current_lv);

                    if (s_res < 0) {
                        break;  // Error found, end.
                    }

                    // s is now also on the next level. Continue comparison.
                    continue;
                } else {
                    // s is longer than t (and t_prefix isn't set).
                    return 1;
                }
            }

            if (sscanner.get_weight_level() > current_lv) {
                // s ran out of weights on this level, and t didn't.
                return -1;
            }

            break;
        }

        return (s_res - t_res);
    }

    @Override
    public int compareSp(Slice str1, Slice str2) {
        return compare(str1, str2);
    }

    class ZhUcaScanner {
        private final int nochar[] = {0, 0};

        MCType.CHARSET_INFO cs = my_charset_utf8mb4_zh_0900_as_cs;
        StrUcaType.MY_UCA_INFO uca = cs.uca;

        /**
         * 0 = Primary, 1 = Secondary, 2 = Tertiary.
         */
        int weight_lv = 0;
        int levelsForCompare;

        long num_of_ce_left = 0;

        Slice str;

        // Beginning of the current weight string
        int wbeg = 0;
        int[] weightStr = nochar;

        // Number of bytes between weights in string
        int wbeg_stride = 0;

        int sbeg = 0;
        int sbeg_dup = 0;
        int send = 0;

        int implicit[] = new int[10];

        // Previous code point we scanned, if any.
        int prev_char = 0;

        ZhUcaScanner(Slice str) {
            this.str = str;
            this.send = str.length();
            this.levelsForCompare = cs.levels_for_compare;
        }

        public func_lambda new_func_lambda(byte[] dstStr, int dst, int dst_end) {
            return new func_lambda(dstStr, dst, dst_end);
        }

        class func_lambda {
            byte[] dstStr;
            int dst;
            int dst_end;

            public func_lambda(byte[] dstStr, int dst, int dst_end) {
                this.dstStr = dstStr;
                this.dst = dst;
                this.dst_end = dst_end;
            }

            boolean func(int s_res, boolean is_level_separator) {
                assert (is_level_separator == (s_res == 0));
                if (levelsForCompare == 1) {
                    assert (!is_level_separator);
                }

                // dst = store16be(dst, s_res);

                dstStr[dst] = (byte) (s_res >>> 8);
                dstStr[dst + 1] = (byte) s_res;
                dst = dst + 2;

                return dst < dst_end;
            }
        }

        class preaccept_data_lambda {
            byte[] dstStr;
            int dst;
            int dst_end;

            public preaccept_data_lambda(byte[] dstStr, int dst, int dst_end) {
                this.dstStr = dstStr;
                this.dst = dst;
                this.dst_end = dst_end;
            }

            boolean preaccept_data(int num_weights) {
                return (dst < dst_end - num_weights * 2);
            }
        }

        void for_each_weight(
            func_lambda func, preaccept_data_lambda preaccept_data) {
            if (cs.tailoring != null || cs.mbminlen != 1 || cs.coll_param != null) {
                // Slower, generic path.
                int s_res;
                while ((s_res = next()) >= 0) {
                    if (!func.func(s_res, s_res == 0)) {
                        return;
                    }
                }
                return;
            }

            // Don't handle the case that cs.tailoring == null
            throw new UnsupportedOperationException();
        }

        int next() {
            int res = next_raw();
            StrUcaType.Coll_param param = cs.coll_param;
            if (res > 0 && param != null) {
                // Reorder weight change only on primary level.
                if (param.reorder_param != null && weight_lv == 0) {
                    res = apply_reorder_param(res);
                }
                if (param.case_first != StrUcaType.enum_case_first.CASE_FIRST_OFF) {
                    res = apply_case_first(res);
                }
            }
            return res;
        }

        /**
         * Get the level the scanner is currently working on. The string
         * can be scanned multiple times (if the collation requires multi-level
         * comparisons, e.g. for accent or case sensitivity); first to get
         * primary weights, then from the start again for secondary, etc.
         */
        int get_weight_level() {
            return weight_lv;
        }

        int more_weight() {

            // Check if the weights for the previous code point have been
            // already fully scanned. If no, return the first non-zero
            // weight.
            while (num_of_ce_left != 0 && weightStr[wbeg] == 0) {
                wbeg += wbeg_stride;
                --num_of_ce_left;
            }
            if (num_of_ce_left != 0) {
                int rtn = weightStr[wbeg];
                wbeg += wbeg_stride;
                --num_of_ce_left;
                return rtn; /* return the next weight from expansion     */
            }
            return -1;
        }

        int change_zh_implicit(int weight) {
            assert (weight >= 0xFB00);
            switch (weight) {
            case 0xFB00:
                return 0xF621;
            case 0xFB40:
                return 0xBDBF;
            case 0xFB41:
                return 0xBDC0;
            case 0xFB80:
                return 0xBDC1;
            case 0xFB84:
                return 0xBDC2;
            case 0xFB85:
                return 0xBDC3;
            default:
                return weight + 0xF622 - 0xFBC0;
            }
        }

        int next_implicit(int ch) {
            // Don't handle HANGUL JAMO.

            // We give the Chinese collation different leading primary weight to make
            // sure there are enough single weight values to be assigned to character
            // groups like Latin, Cyrillic, etc.
            int page = 0;
            if (ch >= 0x17000 && ch <= 0x18AFF)  // Tangut character
            {
                page = 0xFB00;
                implicit[3] = (ch - 0x17000) | 0x8000;
            } else {
                page = ch >> 15;
                implicit[3] = (ch & 0x7FFF) | 0x8000;
                if ((ch >= 0x3400 && ch <= 0x4DB5) || (ch >= 0x20000 && ch <= 0x2A6D6) ||
                    (ch >= 0x2A700 && ch <= 0x2B734) || (ch >= 0x2B740 && ch <= 0x2B81D) ||
                    (ch >= 0x2B820 && ch <= 0x2CEA1)) {
                    page += 0xFB80;
                } else if ((ch >= 0x4E00 && ch <= 0x9FD5) || (ch >= 0xFA0E && ch <= 0xFA29)) {
                    page += 0xFB40;
                } else {
                    page += 0xFBC0;
                }
            }
            if (cs.coll_param == zh_coll_param) {
                page = change_zh_implicit(page);
            }
            implicit[0] = page;
            implicit[1] = 0x0020;
            implicit[2] = 0x0002;
            // implicit[3] is set above.
            implicit[4] = 0;
            implicit[5] = 0;
            num_of_ce_left = 1;
            wbeg = MY_UCA_900_CE_SIZE + weight_lv;
            weightStr = implicit;
            wbeg_stride = MY_UCA_900_CE_SIZE;

            return implicit[weight_lv];
        }

        boolean my_uca_have_contractions(StrUcaType.MY_UCA_INFO uca) {
            return uca.have_contractions;
        }

        boolean my_uca_can_be_previous_context_tail(
            byte[] flags, int wc) {
            return (flags[wc & MY_UCA_CNT_FLAG_MASK] & MY_UCA_PREVIOUS_CONTEXT_TAIL) != 0;
        }

        boolean my_uca_can_be_previous_context_head(
            byte[] flags, int wc) {
            return (flags[wc & MY_UCA_CNT_FLAG_MASK] & MY_UCA_PREVIOUS_CONTEXT_HEAD) != 0;
        }

        boolean my_uca_can_be_contraction_head(byte[] flags, int wc) {
            return (flags[wc & MY_UCA_CNT_FLAG_MASK] & MY_UCA_CNT_HEAD) != 0;
        }

        MY_CONTRACTION find_contraction_part_in_trie(
            List<MY_CONTRACTION> cont_nodes, int ch) {
            if (cont_nodes == null || cont_nodes.isEmpty()) {
                return null;
            }
            for (int i = 0; i < cont_nodes.size(); i++) {
                if (cont_nodes.get(i).ch >= ch) {
                    return cont_nodes.get(i);
                }
            }
            return null;
        }

        Object[] previous_context_find(int wc0, int wc1) {
            MY_CONTRACTION node_it1 = find_contraction_part_in_trie(uca.contraction_nodes, wc1);
            if (node_it1 == null || node_it1.ch != wc1) {
                return null;
            }

            MY_CONTRACTION node_it2 =
                find_contraction_part_in_trie(node_it1.child_nodes_context, wc0);
            if (node_it2 != null && node_it2.ch == wc0) {
                if (uca.version == UCA_V900) {
                    weightStr = node_it2.weight;
                    wbeg = MY_UCA_900_CE_SIZE + weight_lv;
                    wbeg_stride = MY_UCA_900_CE_SIZE;
                    num_of_ce_left = 7;
                } else {
                    weightStr = node_it2.weight;
                    wbeg = 1;
                    wbeg_stride = MY_UCA_900_CE_SIZE;
                }
                // index = weight_lv
                return new Object[] {node_it2.weight, weight_lv};
            }
            return null;
        }

        Object[] contraction_find(int wc0, int[] chars_skipped) {
            int beg = 0;
            int s = sbeg;

            List<MY_CONTRACTION> cont_nodes = uca.contraction_nodes;
            MY_CONTRACTION longest_contraction = null;
            MY_CONTRACTION node_it;
            for (; ; ) {
                node_it = find_contraction_part_in_trie(cont_nodes, wc0);
                if (node_it == null || node_it.ch != wc0) {
                    break;
                }
                if (node_it.is_contraction_tail) {
                    longest_contraction = node_it;
                    beg = s;
                    chars_skipped[0] = node_it.contraction_len - 1;
                }
                int mblen;

                int[] parsedWildCharacter = new int[1];

                mblen = codepointOfUTF8(parsedWildCharacter, str, s, send, true, true);
                wc0 = parsedWildCharacter[0];

                if (mblen <= 0) {
                    break;
                }
                s += mblen;
                cont_nodes = node_it.child_nodes;
            }

            if (longest_contraction != null) {
                int[] cweightStr = longest_contraction.weight;
                int cweight = 0;
                if (uca.version == UCA_V900) {
                    cweight += weight_lv;
                    weightStr = cweightStr;
                    wbeg = cweight + MY_UCA_900_CE_SIZE;
                    wbeg_stride = MY_UCA_900_CE_SIZE;
                    num_of_ce_left = 7;
                } else {
                    weightStr = cweightStr;
                    wbeg = cweight + 1;
                    wbeg_stride = MY_UCA_900_CE_SIZE;
                }
                sbeg = beg;
                return new Object[] {cweightStr, cweight};
            }
            return null; /* No contractions were found */
        }

        int next_raw() {
            int remain_weight = more_weight();
            if (remain_weight >= 0) {
                return remain_weight;
            }

            do {
                int wc = 0;
                int[] parsedWildCharacter = new int[1];

                /* Get next code point */

                int mblen = codepointOfUTF8(parsedWildCharacter, str, sbeg, send, true, true);
                wc = parsedWildCharacter[0];
                if (mblen <= 0) {
                    if (levelsForCompare == 1) {
                        ++weight_lv;
                        return -1;
                    }

                    if (++weight_lv < levelsForCompare) {
                        // Don't handle: levelsForCompare == 4 && cs.coll_param == &ja_coll_param

                        // Restart scanning from the beginning of the string, and add
                        // a level separator.
                        sbeg = sbeg_dup;
                        return 0;
                    }

                    // If we don't have any more levels left, we're done.
                    return -1;
                }

                sbeg += mblen;
                assert (wc <= uca.maxchar);  // mb_wc() has already checked this.

                if (my_uca_have_contractions(uca)) {
                    int[] cweightStr = null;
                    int cweight = 0;
                    Object[] cweightPointer = null;

                    // If we have scanned a code point which can have previous context,
                    // and there were some more code points already before,
                    // then verify that {prev_char, wc} together form
                    // a real previous context pair.
                    // Note, we support only 2-character long sequences with previous
                    // context at the moment. CLDR does not have longer sequences.
                    // CLDR doesn't have previous context rule whose first character is
                    // 0x0000, so the initial value (0) of prev_char won't break the logic.

                    if (my_uca_can_be_previous_context_tail(uca.contraction_flags, wc) &&
                        my_uca_can_be_previous_context_head(uca.contraction_flags, prev_char) &&
                        (cweightPointer = previous_context_find(prev_char, wc)) != null) {
                        // Don't Handle: For Japanese kana-sensitive collation.

                        prev_char = 0; /* Clear for the next code point */

                        cweightStr = (int[]) cweightPointer[0];
                        cweight = (int) cweightPointer[1];
                        return cweightStr[cweight];
                    } else if (my_uca_can_be_contraction_head(uca.contraction_flags, wc)) {
                        /* Check if wc starts a contraction */
                        int[] chars_skipped = new int[1];  // Ignored.
                        if ((cweightPointer = contraction_find(wc, chars_skipped)) != null) {
                            cweightStr = (int[]) cweightPointer[0];
                            cweight = (int) cweightPointer[1];
                            return cweightStr[cweight];
                        }

                    }
                    prev_char = wc;
                }

                // Don't handle: For Japanese kana-sensitive collation.

                // Process single code point
                int page = wc >> 8;
                int code = wc & 0xFF;

                // If weight page for wc does not exist, then calculate algorithmically
                int[] wpage = uca.weights[page];
                if (wpage == null) {
                    return next_implicit(wc);
                }

                // Calculate pointer to wc's weight, using page and offset
                wbeg = UCA900_WEIGHT_ADDR(weight_lv, code);
                weightStr = wpage;
                wbeg_stride = UCA900_DISTANCE_BETWEEN_WEIGHTS;
                num_of_ce_left = wpage[code];
            } while (weightStr[wbeg + 0] == 0); // Skip ignorable code points

            int rtn = weightStr[wbeg];
            wbeg += wbeg_stride;
            --num_of_ce_left;
            return rtn;
        }

        int apply_reorder_param(int weight) {
            // Chinese collation's reordering is done in next_implicit() and
            // modify_all_zh_pages(). See the comment on zh_reorder_param and
            // change_zh_implicit().
            if (cs.coll_param == zh_coll_param) {
                return weight;
            }
            throw new UnsupportedOperationException();
        }

        int apply_case_first(int weight) {
            // We only apply case weight change here when the character is not tailored.
            // Tailored character's case weight has been changed in
            // my_char_weight_put_900().
            // We have only 1 collation (Danish) needs to implement [caseFirst upper].
            if (cs.coll_param.case_first == CASE_FIRST_UPPER && weight_lv == 2 &&
                weight < 0x20) {
                if (is_tertiary_weight_upper_case(weight)) {
                    weight |= Uca900Data.CASE_FIRST_UPPER_MASK;
                } else {
                    weight |= Uca900Data.CASE_FIRST_LOWER_MASK;
                }
            }
            return weight;
        }

        // See Unicode TR35 section 3.14.1.
        boolean is_tertiary_weight_upper_case(int weight) {
            if ((weight >= 0x08 && weight <= 0x0C) || weight == 0x0E || weight == 0x11 ||
                weight == 0x12 || weight == 0x1D) {
                return true;
            }
            return false;
        }
    }

    /**
     * In a loop, scans weights from the source string and writes
     * them into the binary image. In a case insensitive collation,
     * upper and lower cases of the same letter will produce the
     * same image subsequences. When we have reached the end-of-string
     * or found an illegal multibyte sequence, the loop stops.
     * <p>
     * It is impossible to restore the original string using its
     * binary image.
     * <p>
     * Binary images are used for bulk comparison purposes,
     * e.g. in ORDER BY, when it is more efficient to create
     * a binary image and use it instead of weight scanner
     * for the original strings for every comparison.
     */
    @Override
    public SortKey getSortKey(Slice str, int maxLength) {
        byte[] dstStr = new byte[maxLength];
        int dst = 0;
        int d0 = 0;
        int dst_end = maxLength;

        ZhUcaScanner scanner = new ZhUcaScanner(str);

        assert ((maxLength % 2) == 0);
        if ((maxLength % 2) == 1) {
            // Emergency workaround for optimized mode.
            --dst_end;
        }

        if (dst != dst_end) {
            ZhUcaScanner.func_lambda func_lambda = scanner.new_func_lambda(dstStr, dst, dst_end);

            scanner.for_each_weight(func_lambda, null);

            dst = func_lambda.dst;
            dst_end = func_lambda.dst_end;
        }

        Arrays.fill(dstStr, dst, dst_end, (byte) 0);
        dst = dst_end;

        return new SortKey(getCharsetName(), getName(), str.getInput(), dstStr);
    }

    /**
     * ctype-uca.c my_hash_sort_uca:
     * Calculates hash value for the given string, according to the collation, and ignoring trailing spaces.
     * <p>
     * Warning: equality inconsistency for invalid characters.
     */
    @Override
    public int hashcode(Slice str) {
        long n1 = 1L;
        ZhUcaScanner scanner = new ZhUcaScanner(str);

        long h = n1;
        h ^= 0xcbf29ce484222325L;

        int s_res;
        while ((s_res = scanner.next()) >= 0) {
            h ^= s_res;
            h *= 0x100000001B3L;
        }
        return (int) h;
    }

    @Override
    public void hashcode(byte[] bytes, int begin, int end, long[] numbers) {
        long n1 = numbers[0];
        Slice str = Slices.wrappedBuffer(bytes, begin, end - begin);
        ZhUcaScanner scanner = new ZhUcaScanner(str);

        long h = n1;
        h ^= 0xcbf29ce484222325L;

        int s_res;
        while ((s_res = scanner.next()) >= 0) {
            h ^= s_res;
            h *= 0x100000001B3L;
        }
        numbers[0] = h;
    }

    public int getCodePoint(SliceInput sliceInput) {
        return codepointOfUTF8(sliceInput);
    }
}
