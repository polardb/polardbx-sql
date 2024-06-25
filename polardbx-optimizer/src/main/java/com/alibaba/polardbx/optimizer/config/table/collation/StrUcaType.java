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

import java.util.List;

public class StrUcaType {
    public static final int MY_UCA_CNT_FLAG_SIZE = 4096;
    public static final int MY_UCA_CNT_FLAG_MASK = 4095;
    public static final int UCA_MAX_CHAR_GRP = 4;
    public static final int MY_UCA_PREVIOUS_CONTEXT_TAIL = 128;
    public static final int MY_UCA_PREVIOUS_CONTEXT_HEAD = 64;
    public static final int MY_UCA_CNT_HEAD = 1;
    public static final int MY_UCA_CNT_TAIL = 2;

    public static class Coll_param {
        Reorder_param reorder_param;
        boolean norm_enabled;  // false = normalization off, default; true = on
        enum_case_first case_first;

        public Coll_param(Reorder_param reorder_param, boolean norm_enabled, enum_case_first case_first) {
            this.reorder_param = reorder_param;
            this.norm_enabled = norm_enabled;
            this.case_first = case_first;
        }
    }

    enum enum_case_first {CASE_FIRST_OFF, CASE_FIRST_UPPER, CASE_FIRST_LOWER}

    public static class Reorder_param {
        enum_char_grp[] reorder_grp = new enum_char_grp[UCA_MAX_CHAR_GRP];
        Reorder_wt_rec[] wt_rec = new Reorder_wt_rec[2 * UCA_MAX_CHAR_GRP];
        int wt_rec_num;
        int max_weight;

        public Reorder_param(enum_char_grp[] reorder_grp, Reorder_wt_rec[] wt_rec, int wt_rec_num, int max_weight) {
            this.reorder_grp = reorder_grp;
            this.wt_rec = wt_rec;
            this.wt_rec_num = wt_rec_num;
            this.max_weight = max_weight;
        }
    }

    enum enum_char_grp {
        CHARGRP_NONE,
        CHARGRP_CORE,
        CHARGRP_LATIN,
        CHARGRP_CYRILLIC,
        CHARGRP_ARAB,
        CHARGRP_KANA,
        CHARGRP_OTHERS
    }

    public static class Reorder_wt_rec {
        Weight_boundary old_wt_bdy;
        Weight_boundary new_wt_bdy;

        public Reorder_wt_rec(Weight_boundary old_wt_bdy, Weight_boundary new_wt_bdy) {
            this.old_wt_bdy = old_wt_bdy;
            this.new_wt_bdy = new_wt_bdy;
        }
    }

    public static class Weight_boundary {
        int begin;
        int end;

        public Weight_boundary(int begin, int end) {
            this.begin = begin;
            this.end = end;
        }
    }

    enum enum_uca_ver {UCA_V400, UCA_V520, UCA_V900}

    public static final int MY_UCA_MAX_WEIGHT_SIZE = 25;

    /**
     * We store all the contractions in a trie, indexed on the codepoints they
     * consist of. The trie is organized as:
     * 1. Each node stores one code point (ch) of contraction, and a list of nodes
     * (child_nodes) store all possible following code points.
     * 2. The vector in MY_UCA_INFO stores a list of nodes which store the first
     * code points of all contractions.
     * 3. Each node has a boolean value (is_contraction_tail) which shows
     * whether the code point stored in the node is the end of a contraction.
     * This is necessary because even if one code point is the end of a
     * contraction, there might be longer contraction contains all the
     * code points in the path (e.g., for Hungarian, both 'DZ' and 'DZS' are
     * contractions).
     * 4. A contraction is formed by all the code points in the path until the
     * end of the contraction.
     * 5. If it is the end of a contraction (is_contraction_tail == true), the
     * weight of this contraction is stored in array weight.
     * 6. If it is the end of a contraction (is_contraction_tail == true),
     * with_context shows whether it is common contraction (with_context ==
     * false), or previous context contraction (with_context == true).
     * 7. If it is the end of a contraction (is_contraction_tail == true),
     * contraction_len shows how many code points this contraction consists of.
     */
    public static class MY_CONTRACTION {
        long ch;
        // Lists of following nodes.
        List<MY_CONTRACTION> child_nodes;
        List<MY_CONTRACTION> child_nodes_context;

        // weight and with_context are only useful when is_contraction_tail is true.
        int[] weight = new int[MY_UCA_MAX_WEIGHT_SIZE]; /* Its weight string, 0-terminated */
        boolean is_contraction_tail;
        int contraction_len;

        public MY_CONTRACTION(long ch, List<MY_CONTRACTION> child_nodes, List<MY_CONTRACTION> child_nodes_context,
                              int[] weight,
                              boolean is_contraction_tail, int contraction_len) {
            this.ch = ch;
            this.child_nodes = child_nodes;
            this.child_nodes_context = child_nodes_context;
            this.weight = weight;
            this.is_contraction_tail = is_contraction_tail;
            this.contraction_len = contraction_len;
        }
    }

    public static class MY_UCA_INFO {
        enum_uca_ver version = enum_uca_ver.UCA_V400;
        MY_UCA_INFO m_based_on = null;

        // Collation weights.
        long maxchar = 0;

        int[] lengths = null;
        List<Integer> m_allocated_weights = null;
        int[][] weights = null;

        boolean have_contractions = false;
        List<MY_CONTRACTION> contraction_nodes = null;
        /**
         * contraction_flags is only used when a collation has contraction rule.
         * UCA collation supports at least 65535 characters, but only a few of
         * them can be part of contraction, it is huge waste of time to find out
         * whether one character is in contraction list for every character.
         * contraction_flags points to memory which is allocated when a collation
         * has contraction rule. For a character in contraction, its corresponding
         * byte (contraction_flags[ch & 0x1000]) will be set to a certain value
         * according to the position (head, tail or middle) of this character in
         * contraction. This byte will be used to quick check whether one character
         * can be part of contraction.
         */

        byte[] contraction_flags = null;

        /* Logical positions */
        long first_non_ignorable = 0;
        long last_non_ignorable = 0;
        long first_primary_ignorable = 0;
        long last_primary_ignorable = 0;
        long first_secondary_ignorable = 0;
        long last_secondary_ignorable = 0;
        long first_tertiary_ignorable = 0;
        long last_tertiary_ignorable = 0;
        long first_trailing = 0;
        long last_trailing = 0;
        long first_variable = 0;
        long last_variable = 0;

        /**
         * extra_ce_pri_base, extra_ce_sec_base and extra_ce_ter_base are only used for
         * the UCA collations whose UCA version is not smaller than UCA_V900. For why
         * we need this extra CE, please see the comment in my_char_weight_put_900()
         * and apply_primary_shift_900().
         * <p>
         * The value of these three variables is set by the definition of my_uca_v900.
         * The value of extra_ce_pri_base is usually 0x54A4 (which is the maximum
         * regular weight value pluses one, 0x54A3 + 1 = 0x54A4). But for the Chinese
         * collation, the extra_ce_pri_base needs to change. This is because 0x54A4 has
         * been occupied to do reordering. There might be weight conflict if we still
         * use 0x54A4. Please also see the comment on modify_all_zh_pages().
         */
        int extra_ce_pri_base = 0;  // Primary weight of extra CE
        int extra_ce_sec_base = 0;  // Secondary weight of extra CE
        int extra_ce_ter_base = 0;  // Tertiary weight of extra CE

        public MY_UCA_INFO(enum_uca_ver version, MY_UCA_INFO m_based_on, long maxchar, int[] lengths,
                           List<Integer> m_allocated_weights, int[][] weights, boolean have_contractions,
                           List<MY_CONTRACTION> contraction_nodes, byte[] contraction_flags, long first_non_ignorable,
                           long last_non_ignorable, long first_primary_ignorable, long last_primary_ignorable,
                           long first_secondary_ignorable, long last_secondary_ignorable, long first_tertiary_ignorable,
                           long last_tertiary_ignorable, long first_trailing, long last_trailing, long first_variable,
                           long last_variable, int extra_ce_pri_base, int extra_ce_sec_base, int extra_ce_ter_base) {
            this.version = version;
            this.m_based_on = m_based_on;
            this.maxchar = maxchar;
            this.lengths = lengths;
            this.m_allocated_weights = m_allocated_weights;
            this.weights = weights;
            this.have_contractions = have_contractions;
            this.contraction_nodes = contraction_nodes;
            this.contraction_flags = contraction_flags;
            this.first_non_ignorable = first_non_ignorable;
            this.last_non_ignorable = last_non_ignorable;
            this.first_primary_ignorable = first_primary_ignorable;
            this.last_primary_ignorable = last_primary_ignorable;
            this.first_secondary_ignorable = first_secondary_ignorable;
            this.last_secondary_ignorable = last_secondary_ignorable;
            this.first_tertiary_ignorable = first_tertiary_ignorable;
            this.last_tertiary_ignorable = last_tertiary_ignorable;
            this.first_trailing = first_trailing;
            this.last_trailing = last_trailing;
            this.first_variable = first_variable;
            this.last_variable = last_variable;
            this.extra_ce_pri_base = extra_ce_pri_base;
            this.extra_ce_sec_base = extra_ce_sec_base;
            this.extra_ce_ter_base = extra_ce_ter_base;
        }
    }
}
