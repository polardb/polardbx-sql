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

import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;

public class MCType {
    /**
     * A ure containing data for charset+collation pair implementation.
     * <p>
     * Virtual functions that use this data are collected into separate
     * ures, MY_CHARSET_HANDLER and MY_COLLATION_HANDLER.
     */
    public static class CHARSET_INFO {
        long number;
        long primary_number;
        long binary_number;
        long state;
        String csname;
        String m_coll_name;
        String comment;
        String tailoring;
        StrUcaType.Coll_param coll_param;
        int[] ctype;
        int[] to_lower;
        int[] to_upper;
        int[] sort_order;
        StrUcaType.MY_UCA_INFO uca; /* This can be changed in apply_one_rule() */
        int[] tab_to_uni;
        MY_UNI_IDX tab_from_uni;
        MY_UNICASE_INFO caseinfo;
        lex_state_maps_st state_maps; /* parser internal data */
        int[] ident_map;                   /* parser internal data */
        long strxfrm_multiply;
        int caseup_multiply;
        int casedn_multiply;
        long mbminlen;
        long mbmaxlen;
        long mbmaxlenlen;
        int min_sort_char;
        int max_sort_char; /* For LIKE optimization */
        int pad_char;
        boolean escape_with_backslash_is_dangerous;
        int levels_for_compare;

        CharsetHandler cset;
        CollationHandler coll;

        /**
         * If this collation is PAD_SPACE, it collates as if all inputs were
         * padded with a given number of spaces at the end (see the "num_codepoints"
         * flag to strnxfrm). NO_PAD simply compares unextended strings.
         * <p>
         * Note that this is fundamentally about the behavior of coll->strnxfrm.
         */
        Pad_attribute pad_attribute;

        public CHARSET_INFO(long number, long primary_number, long binary_number, long state, String csname,
                            String m_coll_name,
                            String comment, String tailoring, StrUcaType.Coll_param coll_param, int[] ctype,
                            int[] to_lower,
                            int[] to_upper, int[] sort_order, StrUcaType.MY_UCA_INFO uca, int[] tab_to_uni,
                            MY_UNI_IDX tab_from_uni, MY_UNICASE_INFO caseinfo, lex_state_maps_st state_maps,
                            int[] ident_map,
                            long strxfrm_multiply, int caseup_multiply, int casedn_multiply, long mbminlen,
                            long mbmaxlen,
                            long mbmaxlenlen, int min_sort_char, int max_sort_char, int pad_char,
                            boolean escape_with_backslash_is_dangerous, int levels_for_compare, CharsetHandler cset,
                            CollationHandler coll, Pad_attribute pad_attribute) {
            this.number = number;
            this.primary_number = primary_number;
            this.binary_number = binary_number;
            this.state = state;
            this.csname = csname;
            this.m_coll_name = m_coll_name;
            this.comment = comment;
            this.tailoring = tailoring;
            this.coll_param = coll_param;
            this.ctype = ctype;
            this.to_lower = to_lower;
            this.to_upper = to_upper;
            this.sort_order = sort_order;
            this.uca = uca;
            this.tab_to_uni = tab_to_uni;
            this.tab_from_uni = tab_from_uni;
            this.caseinfo = caseinfo;
            this.state_maps = state_maps;
            this.ident_map = ident_map;
            this.strxfrm_multiply = strxfrm_multiply;
            this.caseup_multiply = caseup_multiply;
            this.casedn_multiply = casedn_multiply;
            this.mbminlen = mbminlen;
            this.mbmaxlen = mbmaxlen;
            this.mbmaxlenlen = mbmaxlenlen;
            this.min_sort_char = min_sort_char;
            this.max_sort_char = max_sort_char;
            this.pad_char = pad_char;
            this.escape_with_backslash_is_dangerous = escape_with_backslash_is_dangerous;
            this.levels_for_compare = levels_for_compare;
            this.cset = cset;
            this.coll = coll;
            this.pad_attribute = pad_attribute;
        }
    }

    enum Pad_attribute {PAD_SPACE, NO_PAD}

    ;

    public static class MY_UNI_IDX {
        int from;
        int to;
        int[] tab;
    }

    public static class MY_UNICASE_INFO {
        int maxchar;
        MY_UNICASE_CHARACTER[][] page;

        public MY_UNICASE_INFO(int maxchar, MY_UNICASE_CHARACTER[][] page) {
            this.maxchar = maxchar;
            this.page = page;
        }
    }

    public static class MY_UNICASE_CHARACTER {
        int toupper;
        int tolower;
        int sort;

        public MY_UNICASE_CHARACTER(int toupper, int tolower, int sort) {
            this.toupper = toupper;
            this.tolower = tolower;
            this.sort = sort;
        }
    }

    public static class lex_state_maps_st {
        my_lex_states[] main_map = new my_lex_states[256];
        hint_lex_char_classes[] hint_map = new hint_lex_char_classes[256];
    }

    enum my_lex_states {
        MY_LEX_START,
        MY_LEX_CHAR,
        MY_LEX_IDENT,
        MY_LEX_IDENT_SEP,
        MY_LEX_IDENT_START,
        MY_LEX_REAL,
        MY_LEX_HEX_NUMBER,
        MY_LEX_BIN_NUMBER,
        MY_LEX_CMP_OP,
        MY_LEX_LONG_CMP_OP,
        MY_LEX_STRING,
        MY_LEX_COMMENT,
        MY_LEX_END,
        MY_LEX_NUMBER_IDENT,
        MY_LEX_INT_OR_REAL,
        MY_LEX_REAL_OR_POINT,
        MY_LEX_BOOL,
        MY_LEX_EOL,
        MY_LEX_LONG_COMMENT,
        MY_LEX_END_LONG_COMMENT,
        MY_LEX_SEMICOLON,
        MY_LEX_SET_VAR,
        MY_LEX_USER_END,
        MY_LEX_HOSTNAME,
        MY_LEX_SKIP,
        MY_LEX_USER_VARIABLE_DELIMITER,
        MY_LEX_SYSTEM_VAR,
        MY_LEX_IDENT_OR_KEYWORD,
        MY_LEX_IDENT_OR_HEX,
        MY_LEX_IDENT_OR_BIN,
        MY_LEX_IDENT_OR_NCHAR,
        MY_LEX_IDENT_OR_DOLLAR_QUOTED_TEXT,
        MY_LEX_STRING_OR_DELIMITER
    }

    enum hint_lex_char_classes {
        HINT_CHR_ASTERISK,     // [*]
        HINT_CHR_AT,           // [@]
        HINT_CHR_BACKQUOTE,    // [`]
        HINT_CHR_CHAR,         // default state
        HINT_CHR_DIGIT,        // [[:digit:]]
        HINT_CHR_DOT,          // '.'
        HINT_CHR_DOUBLEQUOTE,  // ["]
        HINT_CHR_EOF,          // pseudo-class
        HINT_CHR_IDENT,        // [_$[:alpha:]]
        HINT_CHR_MB,           // multibyte character
        HINT_CHR_NL,           // \n
        HINT_CHR_QUOTE,        // [']
        HINT_CHR_SLASH,        // [/]
        HINT_CHR_SPACE         // [[:space:]] excluding \n
    }

}
