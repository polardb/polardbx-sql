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
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import org.yaml.snakeyaml.Yaml;

import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.MIN_MB_EVEN_BYTE_2;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.MIN_MB_ODD_BYTE;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.getGB180304ChsToDiff;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.isMb1;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.isMbEven2;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.isMbEven4;
import static com.alibaba.polardbx.optimizer.config.table.collation.Gb18030ChineseCiCollationHandler.isMbOdd;

public class Gb18030Unicode520CiCollationHandler extends AbstractUCA520CollationHandler {
    public Gb18030Unicode520CiCollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    private static final int[] TAB_GB18030_2_UNI;
    private static final int[] TAB_GB18030_4_UNI;

    static {
        Yaml yaml = new Yaml();
        TAB_GB18030_2_UNI =
            yaml.loadAs(Gb18030Unicode520CiCollationHandler.class.getResourceAsStream("gb18030_2_uni_tab.yml"),
                int[].class);
        TAB_GB18030_4_UNI =
            yaml.loadAs(Gb18030Unicode520CiCollationHandler.class.getResourceAsStream("gb18030_4_uni_tab.yml"),
                int[].class);
    }

    @Override
    public CollationName getName() {
        return CollationName.GB18030_UNICODE_520_CI;
    }

    @Override
    public CharsetName getCharsetName() {
        return CharsetName.GB18030;
    }

    @Override
    int getCodePoint(SliceInput sliceInput) {
        return codeOfGB18030(sliceInput);
    }

    @Override
    public int instr(Slice source, Slice target) {
        return instrForMultiBytes(source, target, Gb18030Unicode520CiCollationHandler::codeOfGB18030);
    }

    public static int codeOfGB18030(SliceInput sliceInput) {
        if (sliceInput.available() <= 0) {
            return INVALID_CODE;
        }

        int idx = 0;
        int cp = 0;
        byte b1 = sliceInput.readByte();

        if (isMb1(b1)) {
            /* [0x00, 0x7F] */
            return Byte.toUnsignedInt(b1);
        } else if (!isMbOdd(b1)) {
            return INVALID_CODE;
        }

        if (!sliceInput.isReadable()) {
            return INVALID_CODE;
        }

        byte b2 = sliceInput.readByte();
        if (isMbEven2(b2)) {
            idx = (Byte.toUnsignedInt(b1) - MIN_MB_ODD_BYTE) * 192 + (Byte.toUnsignedInt(b2)
                - MIN_MB_EVEN_BYTE_2);
            return TAB_GB18030_2_UNI[idx];
        } else if (isMbEven4(b2)) {
            if (sliceInput.available() < 2) {
                return INVALID_CODE;
            }

            byte b3 = sliceInput.readByte();
            byte b4 = sliceInput.readByte();
            if (!(isMbOdd(b3) && isMbEven4(b4))) {
                return INVALID_CODE;
            }

            idx = getGB180304ChsToDiff(Slices.wrappedBuffer(b1, b2, b3, b4));

            if (idx < 0x334) {
                /* [GB+81308130, GB+8130D330) */
                cp = TAB_GB18030_4_UNI[idx];
            } else if (idx <= 0x1D20) {
                /* [GB+8130D330, GB+8135F436] */
                cp = idx + 0x11E;
            } else if (idx < 0x2403) {
                /* (GB+8135F436, GB+8137A839) */
                cp = TAB_GB18030_4_UNI[idx - 6637];
            } else if (idx <= 0x2C40) {
                /* [GB+8137A839, GB+8138FD38] */
                cp = idx + 0x240;
            } else if (idx < 0x4A63) {
                /* (GB+8138FD38, GB+82358F33) */
                cp = TAB_GB18030_4_UNI[idx - 6637 - 2110];
            } else if (idx <= 0x82BC) {
                /* [GB+82358F33, GB+8336C738] */
                cp = idx + 0x5543;
            } else if (idx < 0x830E) {
                /* (GB+8336C738, GB+8336D030) */
                cp = TAB_GB18030_4_UNI[idx - 6637 - 2110 - 14426];
            } else if (idx <= 0x93D4) {
                /* [GB+8336D030, GB+84308534] */
                cp = idx + 0x6557;
            } else if (idx < 0x94BE) {
                /* (GB+84308534, GB+84309C38) */
                cp = TAB_GB18030_4_UNI[idx - 6637 - 2110 - 14426 - 4295];
            } else if (idx <= 0x98C3) {
                /* [GB+84309C38, GB+84318537] */
                cp = idx + 0x656C;
            } else if (idx <= 0x99fb) {
                /* (GB+84318537, GB+8431A439] */
                cp = TAB_GB18030_4_UNI[idx - 6637 - 2110 - 14426 - 4295 - 1030];
            } else if (idx >= 0x2E248 && idx <= 0x12E247) {
                /* [GB+90308130, GB+E3329A35] */
                cp = idx - 0x1E248;
            } else if ((idx > 0x99fb && idx < 0x2E248) ||
                (idx > 0x12E247 && idx <= 0x18398F)) {
                /* (GB+8431A439, GB+90308130) and (GB+E3329A35, GB+FE39FE39) */
                cp = 0x003F;
            } else {
                return INVALID_CODE;
            }

            return cp;
        } else {
            return INVALID_CODE;
        }
    }
}
