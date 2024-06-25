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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.*;
import com.alibaba.polardbx.optimizer.config.table.collation.MCType.*;
import com.google.common.base.Preconditions;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.config.table.collation.StrUcaType.enum_uca_ver.UCA_V900;

public class Uca900Data {
    public static final int CASE_FIRST_UPPER_MASK = 0x0100;
    public static final int CASE_FIRST_MIXED_MASK = 0x0200;
    public static final int CASE_FIRST_LOWER_MASK = 0x0300;

    public static final int MY_UCA_900_CE_SIZE = 3;

    public static int UCA900_WEIGHT_ADDR(int level, int subcode) {
        return 256 + level * 256 + subcode;
    }

    public static int UCA900_NUM_OF_CE(int[] page, int subcode) {
        return page[subcode];
    }

    public static int UCA900_DISTANCE_BETWEEN_WEIGHTS = MY_UCA_900_CE_SIZE * 256;

    public static int[] uca900_p000;
    public static int[] uca900_p001;
    public static int[] uca900_p002;
    public static int[] uca900_p003;
    public static int[] uca900_p004;
    public static int[] uca900_p005;
    public static int[] uca900_p006;
    public static int[] uca900_p007;
    public static int[] uca900_p008;
    public static int[] uca900_p009;
    public static int[] uca900_p00A;
    public static int[] uca900_p00B;
    public static int[] uca900_p00C;
    public static int[] uca900_p00D;
    public static int[] uca900_p00E;
    public static int[] uca900_p00F;
    public static int[] uca900_p010;
    public static int[] uca900_p011;
    public static int[] uca900_p012;
    public static int[] uca900_p013;
    public static int[] uca900_p014;
    public static int[] uca900_p015;
    public static int[] uca900_p016;
    public static int[] uca900_p017;
    public static int[] uca900_p018;
    public static int[] uca900_p019;
    public static int[] uca900_p01A;
    public static int[] uca900_p01B;
    public static int[] uca900_p01C;
    public static int[] uca900_p01D;
    public static int[] uca900_p01E;
    public static int[] uca900_p01F;
    public static int[] uca900_p020;
    public static int[] uca900_p021;
    public static int[] uca900_p022;
    public static int[] uca900_p023;
    public static int[] uca900_p024;
    public static int[] uca900_p025;
    public static int[] uca900_p026;
    public static int[] uca900_p027;
    public static int[] uca900_p028;
    public static int[] uca900_p029;
    public static int[] uca900_p02A;
    public static int[] uca900_p02B;
    public static int[] uca900_p02C;
    public static int[] uca900_p02D;
    public static int[] uca900_p02E;
    public static int[] uca900_p02F;
    public static int[] uca900_p030;
    public static int[] uca900_p031;
    public static int[] uca900_p032;
    public static int[] uca900_p033;
    public static int[] uca900_p04D;
    public static int[] uca900_p0A0;
    public static int[] uca900_p0A1;
    public static int[] uca900_p0A2;
    public static int[] uca900_p0A3;
    public static int[] uca900_p0A4;
    public static int[] uca900_p0A5;
    public static int[] uca900_p0A6;
    public static int[] uca900_p0A7;
    public static int[] uca900_p0A8;
    public static int[] uca900_p0A9;
    public static int[] uca900_p0AA;
    public static int[] uca900_p0AB;
    public static int[] uca900_p0D7;
    public static int[] uca900_p0F9;
    public static int[] uca900_p0FA;
    public static int[] uca900_p0FB;
    public static int[] uca900_p0FC;
    public static int[] uca900_p0FD;
    public static int[] uca900_p0FE;
    public static int[] uca900_p0FF;
    public static int[] uca900_p100;
    public static int[] uca900_p101;
    public static int[] uca900_p102;
    public static int[] uca900_p103;
    public static int[] uca900_p104;
    public static int[] uca900_p105;
    public static int[] uca900_p106;
    public static int[] uca900_p107;
    public static int[] uca900_p108;
    public static int[] uca900_p109;
    public static int[] uca900_p10A;
    public static int[] uca900_p10B;
    public static int[] uca900_p10C;
    public static int[] uca900_p10E;
    public static int[] uca900_p110;
    public static int[] uca900_p111;
    public static int[] uca900_p112;
    public static int[] uca900_p113;
    public static int[] uca900_p114;
    public static int[] uca900_p115;
    public static int[] uca900_p116;
    public static int[] uca900_p117;
    public static int[] uca900_p118;
    public static int[] uca900_p11A;
    public static int[] uca900_p11C;
    public static int[] uca900_p120;
    public static int[] uca900_p121;
    public static int[] uca900_p122;
    public static int[] uca900_p123;
    public static int[] uca900_p124;
    public static int[] uca900_p125;
    public static int[] uca900_p130;
    public static int[] uca900_p131;
    public static int[] uca900_p132;
    public static int[] uca900_p133;
    public static int[] uca900_p134;
    public static int[] uca900_p144;
    public static int[] uca900_p145;
    public static int[] uca900_p146;
    public static int[] uca900_p168;
    public static int[] uca900_p169;
    public static int[] uca900_p16A;
    public static int[] uca900_p16B;
    public static int[] uca900_p16F;
    public static int[] uca900_p1B0;
    public static int[] uca900_p1BC;
    public static int[] uca900_p1D0;
    public static int[] uca900_p1D1;
    public static int[] uca900_p1D2;
    public static int[] uca900_p1D3;
    public static int[] uca900_p1D4;
    public static int[] uca900_p1D5;
    public static int[] uca900_p1D6;
    public static int[] uca900_p1D7;
    public static int[] uca900_p1D8;
    public static int[] uca900_p1D9;
    public static int[] uca900_p1DA;
    public static int[] uca900_p1E0;
    public static int[] uca900_p1E8;
    public static int[] uca900_p1E9;
    public static int[] uca900_p1EE;
    public static int[] uca900_p1F0;
    public static int[] uca900_p1F1;
    public static int[] uca900_p1F2;
    public static int[] uca900_p1F3;
    public static int[] uca900_p1F4;
    public static int[] uca900_p1F5;
    public static int[] uca900_p1F6;
    public static int[] uca900_p1F7;
    public static int[] uca900_p1F8;
    public static int[] uca900_p1F9;
    public static int[] uca900_p2F8;
    public static int[] uca900_p2F9;
    public static int[] uca900_p2FA;
    public static int[] uca900_pE00;
    public static int[] uca900_pE01;

    static {
        Yaml yaml = new Yaml();
        int[][] uca900 =
            yaml.loadAs(AbstractUCACollationHandler.class.getResourceAsStream("uca900.yml"), int[][].class);
        int i = 0;
        uca900_p000 = uca900[i++];
        uca900_p001 = uca900[i++];
        uca900_p002 = uca900[i++];
        uca900_p003 = uca900[i++];
        uca900_p004 = uca900[i++];
        uca900_p005 = uca900[i++];
        uca900_p006 = uca900[i++];
        uca900_p007 = uca900[i++];
        uca900_p008 = uca900[i++];
        uca900_p009 = uca900[i++];
        uca900_p00A = uca900[i++];
        uca900_p00B = uca900[i++];
        uca900_p00C = uca900[i++];
        uca900_p00D = uca900[i++];
        uca900_p00E = uca900[i++];
        uca900_p00F = uca900[i++];
        uca900_p010 = uca900[i++];
        uca900_p011 = uca900[i++];
        uca900_p012 = uca900[i++];
        uca900_p013 = uca900[i++];
        uca900_p014 = uca900[i++];
        uca900_p015 = uca900[i++];
        uca900_p016 = uca900[i++];
        uca900_p017 = uca900[i++];
        uca900_p018 = uca900[i++];
        uca900_p019 = uca900[i++];
        uca900_p01A = uca900[i++];
        uca900_p01B = uca900[i++];
        uca900_p01C = uca900[i++];
        uca900_p01D = uca900[i++];
        uca900_p01E = uca900[i++];
        uca900_p01F = uca900[i++];
        uca900_p020 = uca900[i++];
        uca900_p021 = uca900[i++];
        uca900_p022 = uca900[i++];
        uca900_p023 = uca900[i++];
        uca900_p024 = uca900[i++];
        uca900_p025 = uca900[i++];
        uca900_p026 = uca900[i++];
        uca900_p027 = uca900[i++];
        uca900_p028 = uca900[i++];
        uca900_p029 = uca900[i++];
        uca900_p02A = uca900[i++];
        uca900_p02B = uca900[i++];
        uca900_p02C = uca900[i++];
        uca900_p02D = uca900[i++];
        uca900_p02E = uca900[i++];
        uca900_p02F = uca900[i++];
        uca900_p030 = uca900[i++];
        uca900_p031 = uca900[i++];
        uca900_p032 = uca900[i++];
        uca900_p033 = uca900[i++];
        uca900_p04D = uca900[i++];
        uca900_p0A0 = uca900[i++];
        uca900_p0A1 = uca900[i++];
        uca900_p0A2 = uca900[i++];
        uca900_p0A3 = uca900[i++];
        uca900_p0A4 = uca900[i++];
        uca900_p0A5 = uca900[i++];
        uca900_p0A6 = uca900[i++];
        uca900_p0A7 = uca900[i++];
        uca900_p0A8 = uca900[i++];
        uca900_p0A9 = uca900[i++];
        uca900_p0AA = uca900[i++];
        uca900_p0AB = uca900[i++];
        uca900_p0D7 = uca900[i++];
        uca900_p0F9 = uca900[i++];
        uca900_p0FA = uca900[i++];
        uca900_p0FB = uca900[i++];
        uca900_p0FC = uca900[i++];
        uca900_p0FD = uca900[i++];
        uca900_p0FE = uca900[i++];
        uca900_p0FF = uca900[i++];
        uca900_p100 = uca900[i++];
        uca900_p101 = uca900[i++];
        uca900_p102 = uca900[i++];
        uca900_p103 = uca900[i++];
        uca900_p104 = uca900[i++];
        uca900_p105 = uca900[i++];
        uca900_p106 = uca900[i++];
        uca900_p107 = uca900[i++];
        uca900_p108 = uca900[i++];
        uca900_p109 = uca900[i++];
        uca900_p10A = uca900[i++];
        uca900_p10B = uca900[i++];
        uca900_p10C = uca900[i++];
        uca900_p10E = uca900[i++];
        uca900_p110 = uca900[i++];
        uca900_p111 = uca900[i++];
        uca900_p112 = uca900[i++];
        uca900_p113 = uca900[i++];
        uca900_p114 = uca900[i++];
        uca900_p115 = uca900[i++];
        uca900_p116 = uca900[i++];
        uca900_p117 = uca900[i++];
        uca900_p118 = uca900[i++];
        uca900_p11A = uca900[i++];
        uca900_p11C = uca900[i++];
        uca900_p120 = uca900[i++];
        uca900_p121 = uca900[i++];
        uca900_p122 = uca900[i++];
        uca900_p123 = uca900[i++];
        uca900_p124 = uca900[i++];
        uca900_p125 = uca900[i++];
        uca900_p130 = uca900[i++];
        uca900_p131 = uca900[i++];
        uca900_p132 = uca900[i++];
        uca900_p133 = uca900[i++];
        uca900_p134 = uca900[i++];
        uca900_p144 = uca900[i++];
        uca900_p145 = uca900[i++];
        uca900_p146 = uca900[i++];
        uca900_p168 = uca900[i++];
        uca900_p169 = uca900[i++];
        uca900_p16A = uca900[i++];
        uca900_p16B = uca900[i++];
        uca900_p16F = uca900[i++];
        uca900_p1B0 = uca900[i++];
        uca900_p1BC = uca900[i++];
        uca900_p1D0 = uca900[i++];
        uca900_p1D1 = uca900[i++];
        uca900_p1D2 = uca900[i++];
        uca900_p1D3 = uca900[i++];
        uca900_p1D4 = uca900[i++];
        uca900_p1D5 = uca900[i++];
        uca900_p1D6 = uca900[i++];
        uca900_p1D7 = uca900[i++];
        uca900_p1D8 = uca900[i++];
        uca900_p1D9 = uca900[i++];
        uca900_p1DA = uca900[i++];
        uca900_p1E0 = uca900[i++];
        uca900_p1E8 = uca900[i++];
        uca900_p1E9 = uca900[i++];
        uca900_p1EE = uca900[i++];
        uca900_p1F0 = uca900[i++];
        uca900_p1F1 = uca900[i++];
        uca900_p1F2 = uca900[i++];
        uca900_p1F3 = uca900[i++];
        uca900_p1F4 = uca900[i++];
        uca900_p1F5 = uca900[i++];
        uca900_p1F6 = uca900[i++];
        uca900_p1F7 = uca900[i++];
        uca900_p1F8 = uca900[i++];
        uca900_p1F9 = uca900[i++];
        uca900_p2F8 = uca900[i++];
        uca900_p2F9 = uca900[i++];
        uca900_p2FA = uca900[i++];
        uca900_pE00 = uca900[i++];
        uca900_pE01 = uca900[i++];

        Preconditions.checkArgument(i == 149);
    }

    public static final int[][] uca900_weight = new int[][] {
        uca900_p000, uca900_p001, uca900_p002, uca900_p003, uca900_p004,
        uca900_p005, uca900_p006, uca900_p007, uca900_p008, uca900_p009,
        uca900_p00A, uca900_p00B, uca900_p00C, uca900_p00D, uca900_p00E,
        uca900_p00F, uca900_p010, uca900_p011, uca900_p012, uca900_p013,
        uca900_p014, uca900_p015, uca900_p016, uca900_p017, uca900_p018,
        uca900_p019, uca900_p01A, uca900_p01B, uca900_p01C, uca900_p01D,
        uca900_p01E, uca900_p01F, uca900_p020, uca900_p021, uca900_p022,
        uca900_p023, uca900_p024, uca900_p025, uca900_p026, uca900_p027,
        uca900_p028, uca900_p029, uca900_p02A, uca900_p02B, uca900_p02C,
        uca900_p02D, uca900_p02E, uca900_p02F, uca900_p030, uca900_p031,
        uca900_p032, uca900_p033, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, uca900_p04D, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        uca900_p0A0, uca900_p0A1, uca900_p0A2, uca900_p0A3, uca900_p0A4,
        uca900_p0A5, uca900_p0A6, uca900_p0A7, uca900_p0A8, uca900_p0A9,
        uca900_p0AA, uca900_p0AB, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        uca900_p0D7, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, uca900_p0F9,
        uca900_p0FA, uca900_p0FB, uca900_p0FC, uca900_p0FD, uca900_p0FE,
        uca900_p0FF, uca900_p100, uca900_p101, uca900_p102, uca900_p103,
        uca900_p104, uca900_p105, uca900_p106, uca900_p107, uca900_p108,
        uca900_p109, uca900_p10A, uca900_p10B, uca900_p10C, null,
        uca900_p10E, null, uca900_p110, uca900_p111, uca900_p112,
        uca900_p113, uca900_p114, uca900_p115, uca900_p116, uca900_p117,
        uca900_p118, null, uca900_p11A, null, uca900_p11C,
        null, null, null, uca900_p120, uca900_p121,
        uca900_p122, uca900_p123, uca900_p124, uca900_p125, null,
        null, null, null, null, null,
        null, null, null, null, uca900_p130,
        uca900_p131, uca900_p132, uca900_p133, uca900_p134, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, uca900_p144,
        uca900_p145, uca900_p146, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        uca900_p168, uca900_p169, uca900_p16A, uca900_p16B, null,
        null, null, uca900_p16F, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, uca900_p1B0, null, null,
        null, null, null, null, null,
        null, null, null, null, uca900_p1BC,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, uca900_p1D0,
        uca900_p1D1, uca900_p1D2, uca900_p1D3, uca900_p1D4, uca900_p1D5,
        uca900_p1D6, uca900_p1D7, uca900_p1D8, uca900_p1D9, uca900_p1DA,
        null, null, null, null, null,
        uca900_p1E0, null, null, null, null,
        null, null, null, uca900_p1E8, uca900_p1E9,
        null, null, null, null, uca900_p1EE,
        null, uca900_p1F0, uca900_p1F1, uca900_p1F2, uca900_p1F3,
        uca900_p1F4, uca900_p1F5, uca900_p1F6, uca900_p1F7, uca900_p1F8,
        uca900_p1F9, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        uca900_p2F8, uca900_p2F9, uca900_p2FA, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, uca900_pE00,
        uca900_pE01, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
        null, null, null, null, null,
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

    public static MY_UNICASE_CHARACTER[] u900p000 = null;
    public static MY_UNICASE_CHARACTER[] u900p001 = null;
    public static MY_UNICASE_CHARACTER[] u900p002 = null;
    public static MY_UNICASE_CHARACTER[] u900p003 = null;
    public static MY_UNICASE_CHARACTER[] u900p004 = null;
    public static MY_UNICASE_CHARACTER[] u900p005 = null;
    public static MY_UNICASE_CHARACTER[] u900p010 = null;
    public static MY_UNICASE_CHARACTER[] u900p013 = null;
    public static MY_UNICASE_CHARACTER[] u900p01C = null;
    public static MY_UNICASE_CHARACTER[] u900p01D = null;
    public static MY_UNICASE_CHARACTER[] u900p01E = null;
    public static MY_UNICASE_CHARACTER[] u900p01F = null;
    public static MY_UNICASE_CHARACTER[] u900p021 = null;
    public static MY_UNICASE_CHARACTER[] u900p024 = null;
    public static MY_UNICASE_CHARACTER[] u900p02C = null;
    public static MY_UNICASE_CHARACTER[] u900p02D = null;
    public static MY_UNICASE_CHARACTER[] u900p0A6 = null;
    public static MY_UNICASE_CHARACTER[] u900p0A7 = null;
    public static MY_UNICASE_CHARACTER[] u900p0AB = null;
    public static MY_UNICASE_CHARACTER[] u900p0FF = null;
    public static MY_UNICASE_CHARACTER[] u900p104 = null;
    public static MY_UNICASE_CHARACTER[] u900p10C = null;
    public static MY_UNICASE_CHARACTER[] u900p118 = null;
    public static MY_UNICASE_CHARACTER[] u900p1E9 = null;

    public static MY_UNICASE_CHARACTER[][] my_unicase_pages_unicode900 = null;

    static {
        Yaml yaml = new Yaml();
        Map<String, int[][]> arrayMap = new HashMap<>();

        try (InputStream inputStream = Uca900Data.class.getResourceAsStream("uca900_unicase.yml")) {
            Map<String, List<List<Integer>>> yamlData = yaml.load(inputStream);

            for (Map.Entry<String, List<List<Integer>>> entry : yamlData.entrySet()) {
                List<List<Integer>> list2D = entry.getValue();
                int[][] array = new int[list2D.size()][];
                for (int i = 0; i < list2D.size(); i++) {
                    List<Integer> rowList = list2D.get(i);
                    array[i] = rowList.stream().mapToInt(Integer::intValue).toArray();
                }
                arrayMap.put(entry.getKey(), array);
            }

        } catch (FileNotFoundException e) {
            throw GeneralUtil.nestedException("The YAML file was not found: " + e.getMessage());
        } catch (Exception e) {
            throw GeneralUtil.nestedException("Error processing the YAML file: " + e.getMessage());
        }

        // load all planes.
        Map<String, MY_UNICASE_CHARACTER[]> results = new HashMap<>();
        for (Map.Entry<String, int[][]> entry : arrayMap.entrySet()) {
            // for each plane
            int[][] array = entry.getValue();

            MY_UNICASE_CHARACTER[] uniCaseCharacters = new MY_UNICASE_CHARACTER[array.length];

            for (int i = 0; i < array.length; i++) {

                // for each parameter list, build MY_UNICASE_CHARACTER obj.
                uniCaseCharacters[i] = new MY_UNICASE_CHARACTER(array[i][0], array[i][1], array[i][2]);
            }

            results.put(entry.getKey(), uniCaseCharacters);
        }

        u900p000 = results.get("u900p000");
        u900p001 = results.get("u900p001");
        u900p002 = results.get("u900p002");
        u900p003 = results.get("u900p003");
        u900p004 = results.get("u900p004");
        u900p005 = results.get("u900p005");
        u900p010 = results.get("u900p010");
        u900p013 = results.get("u900p013");
        u900p01C = results.get("u900p01C");
        u900p01D = results.get("u900p01D");
        u900p01E = results.get("u900p01E");
        u900p01F = results.get("u900p01F");
        u900p021 = results.get("u900p021");
        u900p024 = results.get("u900p024");
        u900p02C = results.get("u900p02C");
        u900p02D = results.get("u900p02D");
        u900p0A6 = results.get("u900p0A6");
        u900p0A7 = results.get("u900p0A7");
        u900p0AB = results.get("u900p0AB");
        u900p0FF = results.get("u900p0FF");
        u900p104 = results.get("u900p104");
        u900p10C = results.get("u900p10C");
        u900p118 = results.get("u900p118");
        u900p1E9 = results.get("u900p1E9");

        my_unicase_pages_unicode900 = new MY_UNICASE_CHARACTER[][] {
            u900p000, u900p001, u900p002, u900p003, u900p004, u900p005, null,
            null, null, null, null, null, null, null,
            null, null, u900p010, null, null, u900p013, null,
            null, null, null, null, null, null, null,
            u900p01C, u900p01D, u900p01E, u900p01F, null, u900p021, null,
            null, u900p024, null, null, null, null, null,
            null, null, u900p02C, u900p02D, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, u900p0A6, u900p0A7,
            null, null, null, u900p0AB, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, u900p0FF, null, null, null,
            null, u900p104, null, null, null, null, null,
            null, null, u900p10C, null, null, null, null,
            null, null, null, null, null, null, null,
            u900p118, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, u900p1E9,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null, null, null,
            null, null, null, null, null,
        };
    }

    public static final MY_UCA_INFO my_uca_v900 = new MY_UCA_INFO(
        UCA_V900,
        null,

        0x10FFFF,                      /* maxchar           */
        null,                       /* length - not used */
        null,                       /* m_allocated_weights */
        uca900_weight, false, null, /* contractions      */
        null,

        0x0009,  /* first_non_ignorable       p != ignore                       */
        0x14646, /* last_non_ignorable        Not a CJK and not UASSIGNED       */

        0x0332,  /* first_primary_ignorable   p == ignore                       */
        0x101FD, /* last_primary_ignorable                                      */

        0x0000, /* first_secondary_ignorable p,s= ignore                       */
        0xFE73, /* last_secondary_ignorable                                    */

        0x0000, /* first_tertiary_ignorable  p,s,t == ignore                   */
        0xFE73, /* last_tertiary_ignorable                                     */

        0x0000, /* first_trailing                                              */
        0x0000, /* last_trailing                                               */

        0x0009,  /* first_variable            if alt=non-ignorable: p != ignore */
        0x1D371, /* last_variable             if alt=shifter: p,s,t == ignore   */
    /*
      By definition of DUCET 9.0.0, the weight range is:
        Primary weight range:   0200..54A3 (21156)
        Secondary weight range: 0020..0114 (245)
        Variant secondaries:    0110..0114 (5)
        Tertiary weight range:  0002..001F (30)
      To make the extra CE's weight bigger than any weight else, we define it
      as the 'max + 1'.
    */
        0x54A4, /* extra_ce_pri_base  */
        0x0115, /* extra_ce_sec_base  */
        0x0020 /* extra_ce_ter_base  */
    );

    public static final MY_UNICASE_INFO my_unicase_unicode900 = new MY_UNICASE_INFO(
        0x10FFFF,
        my_unicase_pages_unicode900
    );
}
