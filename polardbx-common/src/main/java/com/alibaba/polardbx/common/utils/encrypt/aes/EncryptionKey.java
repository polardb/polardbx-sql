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

package com.alibaba.polardbx.common.utils.encrypt.aes;

import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.TD0;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.TD1;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.TE0;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.TE1;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.TE2;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.TE3;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.Td2;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.Td3;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.rcon;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesUtil.getU32;

public class EncryptionKey {
    protected byte[] userKey;

    protected int[] rk;
    protected int round;

    public EncryptionKey(byte[] userKey) {
        int bits = userKey.length * 8;
        switch (bits) {
        case 128:
            this.round = 10;
            break;
        case 192:
            this.round = 12;
            break;
        case 256:
            this.round = 14;
            break;
        default:
            throw new IllegalArgumentException("Unsupported key length: " +
                bits + ". Should be 128/192/256");
        }
        this.userKey = userKey;
        this.rk = new int[60];
    }

    public void init() {
        int i = 0;
        int temp;

        rk[0] = getU32(userKey, 0);
        rk[1] = getU32(userKey, 4);
        rk[2] = getU32(userKey, 8);
        rk[3] = getU32(userKey, 12);
        int offset;
        if (round == 10) {
            offset = 0;
            while (true) {
                temp = rk[3 + offset];
                rk[4 + offset] = rk[0 + offset] ^
                    (TE2[(temp >>> 16) & 0xff] & 0xff000000) ^
                    (TE3[(temp >>> 8) & 0xff] & 0x00ff0000) ^
                    (TE0[(temp) & 0xff] & 0x0000ff00) ^
                    (TE1[(temp >>> 24)] & 0x000000ff) ^
                    rcon[i];
                rk[5 + offset] = rk[1 + offset] ^ rk[4 + offset];
                rk[6 + offset] = rk[2 + offset] ^ rk[5 + offset];
                rk[7 + offset] = rk[3 + offset] ^ rk[6 + offset];
                if (++i == 10) {
                    return;
                }
                offset += 4;
            }
        }

        rk[4] = getU32(userKey, 16);
        rk[5] = getU32(userKey, 20);
        if (round == 12) {
            offset = 0;
            while (true) {
                temp = rk[5 + offset];
                rk[6 + offset] = rk[0 + offset] ^
                    (TE2[(temp >>> 16) & 0xff] & 0xff000000) ^
                    (TE3[(temp >>> 8) & 0xff] & 0x00ff0000) ^
                    (TE0[(temp) & 0xff] & 0x0000ff00) ^
                    (TE1[(temp >>> 24)] & 0x000000ff) ^
                    rcon[i];
                rk[7 + offset] = rk[1 + offset] ^ rk[6 + offset];
                rk[8 + offset] = rk[2 + offset] ^ rk[7 + offset];
                rk[9 + offset] = rk[3 + offset] ^ rk[8 + offset];
                if (++i == 8) {
                    return;
                }
                rk[10 + offset] = rk[4 + offset] ^ rk[9 + offset];
                rk[11 + offset] = rk[5 + offset] ^ rk[10 + offset];
                offset += 6;
            }
        }
        rk[6] = getU32(userKey, 24);
        rk[7] = getU32(userKey, 28);
        if (round == 14) {
            offset = 0;
            while (true) {
                temp = rk[7 + offset];
                rk[8 + offset] = rk[0 + offset] ^
                    (TE2[(temp >>> 16) & 0xff] & 0xff000000) ^
                    (TE3[(temp >>> 8) & 0xff] & 0x00ff0000) ^
                    (TE0[(temp) & 0xff] & 0x0000ff00) ^
                    (TE1[(temp >>> 24)] & 0x000000ff) ^
                    rcon[i];
                rk[9 + offset] = rk[1 + offset] ^ rk[8 + offset];
                rk[10 + offset] = rk[2 + offset] ^ rk[9 + offset];
                rk[11 + offset] = rk[3 + offset] ^ rk[10 + offset];
                if (++i == 7) {
                    return;
                }
                temp = rk[11 + offset];
                rk[12 + offset] = rk[4 + offset] ^
                    (TE2[(temp >>> 24)] & 0xff000000) ^
                    (TE3[(temp >>> 16) & 0xff] & 0x00ff0000) ^
                    (TE0[(temp >>> 8) & 0xff] & 0x0000ff00) ^
                    (TE1[(temp) & 0xff] & 0x000000ff);
                rk[13 + offset] = rk[5 + offset] ^ rk[12 + offset];
                rk[14 + offset] = rk[6 + offset] ^ rk[13 + offset];
                rk[15 + offset] = rk[7 + offset] ^ rk[14 + offset];

                offset += 8;
            }
        }
    }

    public void invertKey() {
        int tmp;

        for (int i = 0, j = 4 * round; i < j; i += 4, j -= 4) {
            tmp = rk[i];
            rk[i] = rk[j];
            rk[j] = tmp;
            tmp = rk[i + 1];
            rk[i + 1] = rk[j + 1];
            rk[j + 1] = tmp;
            tmp = rk[i + 2];
            rk[i + 2] = rk[j + 2];
            rk[j + 2] = tmp;
            tmp = rk[i + 3];
            rk[i + 3] = rk[j + 3];
            rk[j + 3] = tmp;
        }
        int offset = 0;
        for (int i = 1; i < round; i++) {
            offset += 4;
            rk[0 + offset] =
                TD0[TE1[(rk[0 + offset] >>> 24)] & 0xff] ^
                    TD1[TE1[(rk[0 + offset] >>> 16) & 0xff] & 0xff] ^
                    Td2[TE1[(rk[0 + offset] >>> 8) & 0xff] & 0xff] ^
                    Td3[TE1[(rk[0 + offset]) & 0xff] & 0xff];
            rk[1 + offset] =
                TD0[TE1[(rk[1 + offset] >>> 24)] & 0xff] ^
                    TD1[TE1[(rk[1 + offset] >>> 16) & 0xff] & 0xff] ^
                    Td2[TE1[(rk[1 + offset] >>> 8) & 0xff] & 0xff] ^
                    Td3[TE1[(rk[1 + offset]) & 0xff] & 0xff];
            rk[2 + offset] =
                TD0[TE1[(rk[2 + offset] >>> 24)] & 0xff] ^
                    TD1[TE1[(rk[2 + offset] >>> 16) & 0xff] & 0xff] ^
                    Td2[TE1[(rk[2 + offset] >>> 8) & 0xff] & 0xff] ^
                    Td3[TE1[(rk[2 + offset]) & 0xff] & 0xff];
            rk[3 + offset] =
                TD0[TE1[(rk[3 + offset] >>> 24)] & 0xff] ^
                    TD1[TE1[(rk[3 + offset] >>> 16) & 0xff] & 0xff] ^
                    Td2[TE1[(rk[3 + offset] >>> 8) & 0xff] & 0xff] ^
                    Td3[TE1[(rk[3 + offset]) & 0xff] & 0xff];
        }
    }
}
