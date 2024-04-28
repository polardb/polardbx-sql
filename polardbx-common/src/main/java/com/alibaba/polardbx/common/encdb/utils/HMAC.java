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

package com.alibaba.polardbx.common.encdb.utils;

import com.alibaba.polardbx.common.encdb.enums.HashAlgo;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.digests.SM3Digest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;

/**
 * HMAC
 */
public class HMAC {
    private static byte[] signHmac(byte[] key, byte[] data, Digest digest) {
        KeyParameter keyParameter = new KeyParameter(key);
        HMac hMac = new HMac(digest);

        hMac.init(keyParameter);
        hMac.update(data, 0, data.length);
        byte[] result = new byte[hMac.getMacSize()];
        hMac.doFinal(result, 0);

        return result;
    }

    public static byte[] signWithSM3(byte[] key, byte[] data) {
        return signHmac(key, data, new SM3Digest());
    }

    public static byte[] signWithSHA256(byte[] key, byte[] data) {
        return signHmac(key, data, new SHA256Digest());
    }

    public static byte[] hmac(HashAlgo hashAlg, byte[] key, byte[] data) {
        switch (hashAlg) {
        case SHA256:
            return signWithSHA256(key, data);
        case SM3:
            return signWithSM3(key, data);
        default:
            throw new RuntimeException("Panic! Not support hmac algorithm");
        }
    }
}