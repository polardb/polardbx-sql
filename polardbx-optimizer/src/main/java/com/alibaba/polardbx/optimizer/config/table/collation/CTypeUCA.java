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

import com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.Coll_param;
import com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.Reorder_param;
import com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.Reorder_wt_rec;
import com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.Weight_boundary;
import com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.enum_case_first;
import com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.enum_char_grp;

import static com.alibaba.polardbx.optimizer.config.table.collation.Uca900Data.my_uca_v900;
import static com.alibaba.polardbx.optimizer.config.table.collation.Uca900Data.my_unicase_unicode900;
import static com.alibaba.polardbx.optimizer.config.table.collation.Uca900ZhData.my_uca_v900_zh_cs_as;

public class CTypeUCA {
    public static Reorder_param zh_reorder_param = new Reorder_param(
        new enum_char_grp[] {
            enum_char_grp.CHARGRP_NONE,
            enum_char_grp.CHARGRP_NONE,
            enum_char_grp.CHARGRP_NONE,
            enum_char_grp.CHARGRP_NONE
        },
        new Reorder_wt_rec[] {
            new Reorder_wt_rec(
                new Weight_boundary(0x1C47, 0x54A3),
                new Weight_boundary(0xBDC4, 0xF620)
            ),
            new Reorder_wt_rec(
                new Weight_boundary(0, 0),
                new Weight_boundary(0, 0)
            ),
            new Reorder_wt_rec(
                new Weight_boundary(0, 0),
                new Weight_boundary(0, 0)
            ),
            new Reorder_wt_rec(
                new Weight_boundary(0, 0),
                new Weight_boundary(0, 0)
            ),
            new Reorder_wt_rec(
                new Weight_boundary(0, 0),
                new Weight_boundary(0, 0)
            ),
            new Reorder_wt_rec(
                new Weight_boundary(0, 0),
                new Weight_boundary(0, 0)
            ),
            new Reorder_wt_rec(
                new Weight_boundary(0, 0),
                new Weight_boundary(0, 0)
            ),
            new Reorder_wt_rec(
                new Weight_boundary(0, 0),
                new Weight_boundary(0, 0)
            ),
        },
        1, 0x54A3);

    public static Coll_param zh_coll_param = new Coll_param(
        zh_reorder_param, false, enum_case_first.CASE_FIRST_OFF);

    /* CHARSET_INFO::state flags */
    /* clang-format off */
    static final int MY_CHARSET_UNDEFINED = 0;       // for unit testing
    static final int MY_CS_COMPILED = 1 << 0;  // compiled-in charsets
    static final int MY_CS_CONFIG_UNUSED = 1 << 1;  // unused bitmask
    static final int MY_CS_INDEX_UNUSED = 1 << 2;  // unused bitmask
    static final int MY_CS_LOADED = 1 << 3;  // charsets that are currently loaded
    static final int MY_CS_BINSORT = 1 << 4;  // if binary sort order
    static final int MY_CS_PRIMARY = 1 << 5;  // if primary collation
    static final int MY_CS_STRNXFRM = 1 << 6;  // if _not_ set, sort_order will
    // give same result as strnxfrm --
    // all new collations should have
    // this flag set,
    // do not check it in new code
    static final int MY_CS_UNICODE = 1 << 7;  // if a charset is BMP Unicode
    static final int MY_CS_READY = 1 << 8;  // if a charset is initialized
    static final int MY_CS_AVAILABLE = 1 << 9;  // if either compiled-in or loaded
    static final int MY_CS_CSSORT = 1 << 10; // if case sensitive sort order
    static final int MY_CS_HIDDEN = 1 << 11; // don't display in SHOW
    static final int MY_CS_PUREASCII = 1 << 12; // if a charset is pure ascii
    static final int MY_CS_NONASCII = 1 << 13; // if not ASCII-compatible
    static final int MY_CS_UNICODE_SUPPLEMENT = 1 << 14; // Non-BMP Unicode characters
    static final int MY_CS_LOWER_SORT = 1 << 15; // if use lower case as weight
    static final int MY_CS_INLINE = 1 << 16; // CS definition is C++ source

    /* Character repertoire flags */
    static final int MY_REPERTOIRE_ASCII = 1;     /* Pure ASCII            U+0000..U+007F */
    static final int MY_REPERTOIRE_EXTENDED = 2;  /* Extended characters:  U+0080..U+FFFF */
    static final int MY_REPERTOIRE_UNICODE30 = 3; /* ASCII | EXTENDED:     U+0000..U+FFFF */

    /* Flags for strxfrm */
    static final int MY_STRXFRM_PAD_TO_MAXLEN = 0x00000080; /* if pad tail(for filesort) */

    static final int MY_CS_UTF8MB4_UCA_FLAGS =
        MY_CS_COMPILED | MY_CS_STRNXFRM | MY_CS_UNICODE | MY_CS_UNICODE_SUPPLEMENT;

    /* We consider bytes with code more than 127 as a letter.
     This guarantees that word boundaries work fine with regular
     expressions. Note, there is no need to mark byte 255  as a
     letter, it is illegal byte in UTF8.
     */
    static final int[] ctype_utf8 = {
        0, 32, 32, 32, 32, 32, 32, 32, 32, 32, 40, 40, 40, 40, 40, 32,
        32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
        32, 72, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
        16, 132, 132, 132, 132, 132, 132, 132, 132, 132, 132, 16, 16, 16, 16, 16,
        16, 16, 129, 129, 129, 129, 129, 129, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 16, 16, 16, 16,
        16, 16, 130, 130, 130, 130, 130, 130, 2, 2, 2, 2, 2, 2, 2, 2,
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 16, 16, 16, 16,
        32, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        0};

    public static MCType.CHARSET_INFO my_charset_utf8mb4_zh_0900_as_cs = new MCType.CHARSET_INFO(
        308,
        0,
        0,
        MY_CS_UTF8MB4_UCA_FLAGS | MY_CS_CSSORT,
        "utf8mb4",
        "utf8mb4_zh_0900_as_cs",
        "",
        Uca900ZhData.ZH_CLDR_30,
        CTypeUCA.zh_coll_param,
        ctype_utf8,
        null,
        null,
        null,
        my_uca_v900_zh_cs_as,
        null,
        null,
        my_unicase_unicode900,
        null,
        null,
        0,
        1,
        1,
        1,
        4,
        1,
        32,
        0x10FFFF,
        ' ',
        false,
        3,
        null,
        null,
        MCType.Pad_attribute.NO_PAD
    );
}
