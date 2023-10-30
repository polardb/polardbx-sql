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

import com.alibaba.polardbx.common.charset.SortKey;
import com.alibaba.polardbx.optimizer.config.table.charset.CharsetHandler;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import org.yaml.snakeyaml.Yaml;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;

public abstract class AbstractUCA520CollationHandler extends AbstractCollationHandler {
    AbstractUCA520CollationHandler(CharsetHandler charsetHandler) {
        super(charsetHandler);
    }

    private static final int MAX_CHAR = 0x10FFFF;
    private static final boolean DIFF_IF_ONLY_ENDSPACE_DIFFERENCE = false;

    private static final int FIRST_NON_IGNORABLE = 0x0009;
    private static final int LAST_NON_IGNORABLE =
        0x1342E; /* last_non_ignorable        Not a CJK and not UASSIGNED       */

    private static final int FIRST_PRIMARY_IGNORABLE = 0x0332;
    private static final int LAST_PRIMARY_IGNORABLE = 0x101FD;

    private static final int FIRST_SECONDARY_IGNORABLE = 0x0000;
    private static final int LAST_SECONDARY_IGNORABLE = 0xFE73;

    private static final int FIRST_TERTIARY_IGNORABLE = 0x0000;
    private static final int LAST_TERTIARY_IGNORABLE = 0xFE73;

    private static final int FIRST_TRAILING = 0x0000;
    private static final int LAST_TRAILING = 0x0000;

    private static final int FIRST_VARIABLE = 0x0009;
    private static final int LAST_VARIABLE = 0x1D371;

    private static final int[][] UCA520_WEIGHTS;
    private static final int[] UCA520_LENGTH;

    static {
        Yaml yaml = new Yaml();
        int[][] uca520 =
            yaml.loadAs(AbstractUCA520CollationHandler.class.getResourceAsStream("uca520.yml"), int[][].class);

        int i = 0;
        int[] uca520_p000 = uca520[i++];
        int[] uca520_p001 = uca520[i++];
        int[] uca520_p002 = uca520[i++];
        int[] uca520_p003 = uca520[i++];
        int[] uca520_p004 = uca520[i++];
        int[] uca520_p005 = uca520[i++];
        int[] uca520_p006 = uca520[i++];
        int[] uca520_p007 = uca520[i++];
        int[] uca520_p008 = uca520[i++];
        int[] uca520_p009 = uca520[i++];
        int[] uca520_p00A = uca520[i++];
        int[] uca520_p00B = uca520[i++];
        int[] uca520_p00C = uca520[i++];
        int[] uca520_p00D = uca520[i++];
        int[] uca520_p00E = uca520[i++];
        int[] uca520_p00F = uca520[i++];
        int[] uca520_p010 = uca520[i++];
        int[] uca520_p011 = uca520[i++];
        int[] uca520_p012 = uca520[i++];
        int[] uca520_p013 = uca520[i++];
        int[] uca520_p014 = uca520[i++];
        int[] uca520_p015 = uca520[i++];
        int[] uca520_p016 = uca520[i++];
        int[] uca520_p017 = uca520[i++];
        int[] uca520_p018 = uca520[i++];
        int[] uca520_p019 = uca520[i++];
        int[] uca520_p01A = uca520[i++];
        int[] uca520_p01B = uca520[i++];
        int[] uca520_p01C = uca520[i++];
        int[] uca520_p01D = uca520[i++];
        int[] uca520_p01E = uca520[i++];
        int[] uca520_p01F = uca520[i++];
        int[] uca520_p020 = uca520[i++];
        int[] uca520_p021 = uca520[i++];
        int[] uca520_p022 = uca520[i++];
        int[] uca520_p023 = uca520[i++];
        int[] uca520_p024 = uca520[i++];
        int[] uca520_p025 = uca520[i++];
        int[] uca520_p026 = uca520[i++];
        int[] uca520_p027 = uca520[i++];
        int[] uca520_p028 = uca520[i++];
        int[] uca520_p029 = uca520[i++];
        int[] uca520_p02A = uca520[i++];
        int[] uca520_p02B = uca520[i++];
        int[] uca520_p02C = uca520[i++];
        int[] uca520_p02D = uca520[i++];
        int[] uca520_p02E = uca520[i++];
        int[] uca520_p02F = uca520[i++];
        int[] uca520_p030 = uca520[i++];
        int[] uca520_p031 = uca520[i++];
        int[] uca520_p032 = uca520[i++];
        int[] uca520_p033 = uca520[i++];
        int[] uca520_p04D = uca520[i++];
        int[] uca520_p0A0 = uca520[i++];
        int[] uca520_p0A1 = uca520[i++];
        int[] uca520_p0A2 = uca520[i++];
        int[] uca520_p0A3 = uca520[i++];
        int[] uca520_p0A4 = uca520[i++];
        int[] uca520_p0A5 = uca520[i++];
        int[] uca520_p0A6 = uca520[i++];
        int[] uca520_p0A7 = uca520[i++];
        int[] uca520_p0A8 = uca520[i++];
        int[] uca520_p0A9 = uca520[i++];
        int[] uca520_p0AA = uca520[i++];
        int[] uca520_p0AB = uca520[i++];
        int[] uca520_p0D7 = uca520[i++];
        int[] uca520_p0F9 = uca520[i++];
        int[] uca520_p0FA = uca520[i++];
        int[] uca520_p0FB = uca520[i++];
        int[] uca520_p0FC = uca520[i++];
        int[] uca520_p0FD = uca520[i++];
        int[] uca520_p0FE = uca520[i++];
        int[] uca520_p0FF = uca520[i++];
        int[] uca520_p100 = uca520[i++];
        int[] uca520_p101 = uca520[i++];
        int[] uca520_p102 = uca520[i++];
        int[] uca520_p103 = uca520[i++];
        int[] uca520_p104 = uca520[i++];
        int[] uca520_p108 = uca520[i++];
        int[] uca520_p109 = uca520[i++];
        int[] uca520_p10A = uca520[i++];
        int[] uca520_p10B = uca520[i++];
        int[] uca520_p10C = uca520[i++];
        int[] uca520_p10E = uca520[i++];
        int[] uca520_p110 = uca520[i++];
        int[] uca520_p120 = uca520[i++];
        int[] uca520_p121 = uca520[i++];
        int[] uca520_p122 = uca520[i++];
        int[] uca520_p123 = uca520[i++];
        int[] uca520_p124 = uca520[i++];
        int[] uca520_p130 = uca520[i++];
        int[] uca520_p131 = uca520[i++];
        int[] uca520_p132 = uca520[i++];
        int[] uca520_p133 = uca520[i++];
        int[] uca520_p134 = uca520[i++];
        int[] uca520_p1D0 = uca520[i++];
        int[] uca520_p1D1 = uca520[i++];
        int[] uca520_p1D2 = uca520[i++];
        int[] uca520_p1D3 = uca520[i++];
        int[] uca520_p1D4 = uca520[i++];
        int[] uca520_p1D5 = uca520[i++];
        int[] uca520_p1D6 = uca520[i++];
        int[] uca520_p1D7 = uca520[i++];
        int[] uca520_p1F0 = uca520[i++];
        int[] uca520_p1F1 = uca520[i++];
        int[] uca520_p1F2 = uca520[i++];
        int[] uca520_p2F8 = uca520[i++];
        int[] uca520_p2F9 = uca520[i++];
        int[] uca520_p2FA = uca520[i++];
        int[] uca520_pE00 = uca520[i++];
        int[] uca520_pE01 = uca520[i++];

        UCA520_WEIGHTS = new int[][] {
            uca520_p000, uca520_p001, uca520_p002, uca520_p003, uca520_p004,
            uca520_p005, uca520_p006, uca520_p007, uca520_p008, uca520_p009,
            uca520_p00A, uca520_p00B, uca520_p00C, uca520_p00D, uca520_p00E,
            uca520_p00F, uca520_p010, uca520_p011, uca520_p012, uca520_p013,
            uca520_p014, uca520_p015, uca520_p016, uca520_p017, uca520_p018,
            uca520_p019, uca520_p01A, uca520_p01B, uca520_p01C, uca520_p01D,
            uca520_p01E, uca520_p01F, uca520_p020, uca520_p021, uca520_p022,
            uca520_p023, uca520_p024, uca520_p025, uca520_p026, uca520_p027,
            uca520_p028, uca520_p029, uca520_p02A, uca520_p02B, uca520_p02C,
            uca520_p02D, uca520_p02E, uca520_p02F, uca520_p030, uca520_p031,
            uca520_p032, uca520_p033, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, uca520_p04D, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            uca520_p0A0, uca520_p0A1, uca520_p0A2, uca520_p0A3, uca520_p0A4,
            uca520_p0A5, uca520_p0A6, uca520_p0A7, uca520_p0A8, uca520_p0A9,
            uca520_p0AA, uca520_p0AB, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            uca520_p0D7, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, uca520_p0F9,
            uca520_p0FA, uca520_p0FB, uca520_p0FC, uca520_p0FD, uca520_p0FE,
            uca520_p0FF, uca520_p100, uca520_p101, uca520_p102, uca520_p103,
            uca520_p104, null, null, null, uca520_p108,
            uca520_p109, uca520_p10A, uca520_p10B, uca520_p10C, null,
            uca520_p10E, null, uca520_p110, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, uca520_p120, uca520_p121,
            uca520_p122, uca520_p123, uca520_p124, null, null,
            null, null, null, null, null,
            null, null, null, null, uca520_p130,
            uca520_p131, uca520_p132, uca520_p133, uca520_p134, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, uca520_p1D0,
            uca520_p1D1, uca520_p1D2, uca520_p1D3, uca520_p1D4, uca520_p1D5,
            uca520_p1D6, uca520_p1D7, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, uca520_p1F0, uca520_p1F1, uca520_p1F2, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            uca520_p2F8, uca520_p2F9, uca520_p2FA, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, uca520_pE00,
            uca520_pE01, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null, null, null, null,
            null, null};

        UCA520_LENGTH =
            yaml.loadAs(AbstractUCA520CollationHandler.class.getResourceAsStream("uca520_length.yml"), int[].class);
    }

    @Override
    public CharsetHandler getCharsetHandler() {
        return charsetHandler;
    }

    @Override
    public int compare(Slice str1, Slice str2) {
        Uca520Scanner scanner1 = new Uca520Scanner(str1);
        Uca520Scanner scanner2 = new Uca520Scanner(str2);
        int weight1, weight2;
        do {
            weight1 = scanner1.next();
            weight2 = scanner2.next();
        } while (weight1 == weight2 && weight1 > 0);
        return weight1 - weight2;
    }

    @Override
    public int compareSp(Slice str1, Slice str2) {
        Uca520Scanner scanner1 = new Uca520Scanner(str1);
        Uca520Scanner scanner2 = new Uca520Scanner(str2);
        int weight1, weight2;
        do {
            weight1 = scanner1.next();
            weight2 = scanner2.next();
        } while (weight1 == weight2 && weight1 > 0);

        if (weight1 > 0 && weight2 < 0) {
            // Calculate weight for SPACE character
            weight2 = UCA520_WEIGHTS[0][0x20 * UCA520_LENGTH[0]];

            // compare the first string to spaces
            do {
                if (weight1 != weight2) {
                    return weight1 - weight2;
                }
                weight1 = scanner1.next();
            } while (weight1 > 0);
            return DIFF_IF_ONLY_ENDSPACE_DIFFERENCE ? 1 : 0;
        }

        if (weight1 < 0 && weight2 > 0) {
            // Calculate weight for SPACE character
            weight1 = UCA520_WEIGHTS[0][0x20 * UCA520_LENGTH[0]];

            // compare the first string to spaces
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

    @Override
    public SortKey getSortKey(Slice str, int maxLength) {
        ByteBuffer dst = ByteBuffer.allocate(maxLength);

        Uca520Scanner scanner = new Uca520Scanner(str);

        int weight;
        while (dst.hasRemaining() && (weight = scanner.next()) > 0) {
            dst.put((byte) (weight >> 8));
            if (dst.hasRemaining()) {
                dst.put((byte) (weight & 0xFF));
            }
        }

        int effectiveLen = dst.position();
        if (dst.hasRemaining()) {
            // Calculate weight for SPACE character
            weight = UCA520_WEIGHTS[0][0x20 * UCA520_LENGTH[0]];

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
        // skip trailing space
        while (len >= 1 && str.getByte(len - 1) == 0x20) {
            len--;
        }
        str = str.slice(0, len);
        Uca520Scanner scanner = new Uca520Scanner(str);
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
        // Remove end space. We do this to be able to compare
        // 'A ' and 'A' as identical
        while (end >= begin + 1 && bytes[end - 1] == 0x20) {
            end--;
        }

        long tmp1 = numbers[0] & 0xffffffffL;
        long tmp2 = numbers[1] & 0xffffffffL;

        Slice utf8Str = Slices.wrappedBuffer(bytes, begin, end - begin);
        Uca520Scanner scanner = new Uca520Scanner(utf8Str);
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

    class Uca520Scanner {
        SliceInput inputString;
        IntBuffer weightString;
        int[] implicit;
        int page;
        int code;

        Uca520Scanner(Slice str) {
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
            /*
              Check if the weights for the previous code point have been
              already fully scanned. If yes, then get the next code point and
              initialize wbeg and wlength to its weight string.
            */

            if (weightString.get(weightString.position()) != 0) {    /* More weights left from the previous step: */
                return weightString.get();                          /* return the next weight from expansion     */
            }

            do {
                int codepoint = 0;

                if (!inputString.isReadable()) {
                    return INVALID_CODE;
                }

                /* Get next code point */
                codepoint = getCodePoint(inputString);
                if (codepoint == INVALID_CODE) {
                    return INVALID_CODE;
                }

                if (codepoint > MAX_CHAR) {
                    /* Return 0xFFFD as weight for all characters outside BMP */
                    weightString = noChar();
                    return 0xFFFD;
                }

                /* Process single code point */
                page = codepoint >> 8;
                code = codepoint & 0xFF;

                /* If weight page for wc does not exist, then calculate algorithmically */
                int[] wpage = UCA520_WEIGHTS[page];
                if (wpage == null) {
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

                /* Calculate pointer to wc's weight, using page and offset */
                int index = code * UCA520_LENGTH[page];
                weightString = IntBuffer.wrap(wpage, index, wpage.length - index);
            } while (weightString.get(weightString.position()) == 0); /* Skip ignorable code points */

            return weightString.get();
        }
    }
}
