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

import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.RuntimeCryptoException;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.bouncycastle.crypto.digests.SM3Digest;
import org.bouncycastle.crypto.generators.HKDFBytesGenerator;
import org.bouncycastle.crypto.params.HKDFParameters;

public class HKDF {
    private static byte[] deriveHkdf(int length, byte[] secret, byte[] salt, byte[] info, Digest digest) {
        HKDFBytesGenerator hkdf = new HKDFBytesGenerator(digest);
        HKDFParameters params = new HKDFParameters(secret, salt, info);

        hkdf.init(params);
        byte[] okm = new byte[length];
        hkdf.generateBytes(okm, 0, length);

        return okm;
    }

    public static byte[] deriveWithSHA256(int length, byte[] secret, byte[] salt, byte[] info)
        throws RuntimeCryptoException {
        return deriveHkdf(length, secret, salt, info, new SHA256Digest());
    }

    public static byte[] deriveWithSM3(int length, byte[] secret, byte[] salt, byte[] info)
        throws RuntimeCryptoException {
        return deriveHkdf(length, secret, salt, info, new SM3Digest());
    }
}

