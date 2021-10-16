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

import org.bouncycastle.crypto.BlockCipher;
import org.bouncycastle.crypto.BufferedBlockCipher;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.paddings.PKCS7Padding;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;

import java.util.Arrays;

import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.IV_LENGTH;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.TD0;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.TD1;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.TE0;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.TE1;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.TE2;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.TE3;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.Td2;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.Td3;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.Td4;


public class AesUtil {

    public static byte[] encryptToBytes(BlockEncryptionMode encryptionMode,
                                        byte[] plainTextBytes,
                                        byte[] keyBytes,
                                        byte[] initVector) throws Exception {
        if (!encryptionMode.mode.initVectorRequired) {
            return doEncrypt(encryptionMode, plainTextBytes, keyBytes, null);
        }
        byte[] iv16 = new byte[IV_LENGTH];
        System.arraycopy(initVector, 0, iv16, 0, IV_LENGTH);
        return doEncrypt(encryptionMode, plainTextBytes, keyBytes, iv16);
    }

    public static byte[] decryptToBytes(BlockEncryptionMode encryptionMode,
                                        byte[] crypto,
                                        byte[] keyBytes,
                                        byte[] iv) throws Exception {
        if (!encryptionMode.mode.initVectorRequired) {
            return doDecrypt(encryptionMode, crypto, keyBytes, null);
        }
        byte[] iv16 = new byte[IV_LENGTH];
        System.arraycopy(iv, 0, iv16, 0, IV_LENGTH);
        return doDecrypt(encryptionMode, crypto, keyBytes, iv16);
    }

    private static byte[] doEncrypt(BlockEncryptionMode encryptionMode,
                                    byte[] plainTextBytes,
                                    byte[] keyBytes,
                                    byte[] initVector) throws InvalidCipherTextException {
        byte[] realKey = getRealKey(encryptionMode, keyBytes);

        byte[] crypto;
        if (initVector != null) {
            crypto = innerEncryptWithIv(encryptionMode, plainTextBytes, realKey, initVector);
        } else {
            crypto = innerEncryptWithoutIv(encryptionMode, plainTextBytes, realKey);
        }
        return crypto;
    }

    private static byte[] doDecrypt(BlockEncryptionMode encryptionMode,
                                    byte[] crypto,
                                    byte[] keyBytes,
                                    byte[] iv) throws InvalidCipherTextException {
        byte[] realKey = getRealKey(encryptionMode, keyBytes);
        byte[] plainText;
        if (iv != null) {
            plainText = innerDecryptWithIv(encryptionMode, crypto, realKey, iv);
        } else {
            plainText = innerDecryptWithoutIv(encryptionMode, crypto, realKey);
        }
        return plainText;
    }

    private static byte[] getRealKey(BlockEncryptionMode encryptionMode,
                                     byte[] keyBytes) {
        int realKeyLen = encryptionMode.keylen / 8;
        byte[] realKey = new byte[realKeyLen];
        System.arraycopy(keyBytes, 0, realKey, 0, Math.min(keyBytes.length, realKeyLen));
        return realKey;
    }

    private static byte[] innerEncryptWithIv(BlockEncryptionMode encryptionMode,
                                             byte[] plainTextBytes,
                                             byte[] keyBytes,
                                             byte[] initVector) throws InvalidCipherTextException {
        BufferedBlockCipher bbc = initBBC(encryptionMode, keyBytes, initVector, true);

        byte[] crypto = new byte[bbc.getOutputSize(plainTextBytes.length)];
        int result = bbc.processBytes(plainTextBytes, 0, plainTextBytes.length, crypto, 0);
        bbc.doFinal(crypto, result);
        return crypto;
    }

    private static byte[] innerDecryptWithIv(BlockEncryptionMode encryptionMode,
                                             byte[] crypto,
                                             byte[] keyBytes,
                                             byte[] initVector) throws InvalidCipherTextException {
        BufferedBlockCipher bbc = initBBC(encryptionMode, keyBytes, initVector, false);
        byte[] plainTextByte = new byte[bbc.getOutputSize(crypto.length)];
        final int len1 = bbc.processBytes(crypto, 0, crypto.length, plainTextByte, 0);
        final int len2 = bbc.doFinal(plainTextByte, len1);

        return Arrays.copyOf(plainTextByte, len1 + len2);
    }

    private static BufferedBlockCipher initBBC(BlockEncryptionMode encryptionMode,
                                               byte[] keyBytes,
                                               byte[] initVector,
                                               boolean isEncryption) {
        BlockCipher cipher = AesCipher.getCipher(encryptionMode);
        cipher.reset();
        ParametersWithIV params = new ParametersWithIV(new KeyParameter(keyBytes), initVector);

        BufferedBlockCipher bbc;
        if (encryptionMode.mode == BlockEncryptionMode.Mode.CBC) {

            bbc = new PaddedBufferedBlockCipher(cipher, new PKCS7Padding());
        } else {
            bbc = new BufferedBlockCipher(cipher);
        }
        bbc.init(isEncryption, params);
        return bbc;
    }

    private static byte[] innerEncryptWithoutIv(BlockEncryptionMode encryptionMode,
                                                byte[] plainTextBytes,
                                                byte[] keyBytes) {
        EcbBlockCipher cipher = AesCipher.getEcbCipher();
        cipher.init(true, keyBytes);

        return cipher.doEncrypt(plainTextBytes);
    }

    private static byte[] innerDecryptWithoutIv(BlockEncryptionMode encryptionMode,
                                                byte[] crypto,
                                                byte[] keyBytes) {
        EcbBlockCipher cipher = AesCipher.getEcbCipher();
        cipher.init(false, keyBytes);

        return cipher.doDecrypt(crypto);
    }

    protected static void encryptSingleBlock(byte[] in, int inOff,
                                             byte[] out, int outOff,
                                             EncryptionKey encryptionKey) {
        int[] rk = encryptionKey.rk;
        int s0 = getU32(in, 0 + inOff) ^ rk[0];
        int s1 = getU32(in, 4 + inOff) ^ rk[1];
        int s2 = getU32(in, 8 + inOff) ^ rk[2];
        int s3 = getU32(in, 12 + inOff) ^ rk[3];
        int t0, t1, t2, t3;
        int r = encryptionKey.round >>> 1;
        int offset = 0;
        for (; ; ) {
            t0 =
                TE0[(s0 >>> 24)] ^
                    TE1[(s1 >>> 16) & 0xff] ^
                    TE2[(s2 >>> 8) & 0xff] ^
                    TE3[(s3) & 0xff] ^
                    rk[4 + offset];
            t1 =
                TE0[(s1 >>> 24)] ^
                    TE1[(s2 >>> 16) & 0xff] ^
                    TE2[(s3 >>> 8) & 0xff] ^
                    TE3[(s0) & 0xff] ^
                    rk[5 + offset];
            t2 =
                TE0[(s2 >>> 24)] ^
                    TE1[(s3 >>> 16) & 0xff] ^
                    TE2[(s0 >>> 8) & 0xff] ^
                    TE3[(s1) & 0xff] ^
                    rk[6 + offset];
            t3 =
                TE0[(s3 >>> 24)] ^
                    TE1[(s0 >>> 16) & 0xff] ^
                    TE2[(s1 >>> 8) & 0xff] ^
                    TE3[(s2) & 0xff] ^
                    rk[7 + offset];

            offset += 8;
            if (--r == 0) {
                break;
            }

            s0 =
                TE0[(t0 >>> 24)] ^
                    TE1[(t1 >>> 16) & 0xff] ^
                    TE2[(t2 >>> 8) & 0xff] ^
                    TE3[(t3) & 0xff] ^
                    rk[0 + offset];
            s1 =
                TE0[(t1 >>> 24)] ^
                    TE1[(t2 >>> 16) & 0xff] ^
                    TE2[(t3 >>> 8) & 0xff] ^
                    TE3[(t0) & 0xff] ^
                    rk[1 + offset];
            s2 =
                TE0[(t2 >>> 24)] ^
                    TE1[(t3 >>> 16) & 0xff] ^
                    TE2[(t0 >>> 8) & 0xff] ^
                    TE3[(t1) & 0xff] ^
                    rk[2 + offset];
            s3 =
                TE0[(t3 >>> 24)] ^
                    TE1[(t0 >>> 16) & 0xff] ^
                    TE2[(t1 >>> 8) & 0xff] ^
                    TE3[(t2) & 0xff] ^
                    rk[3 + offset];
        }
        s0 =
            (TE2[(t0 >>> 24)] & 0xff000000) ^
                (TE3[(t1 >>> 16) & 0xff] & 0x00ff0000) ^
                (TE0[(t2 >>> 8) & 0xff] & 0x0000ff00) ^
                (TE1[(t3) & 0xff] & 0x000000ff) ^
                rk[0 + offset];
        putU32(out, 0 + outOff, s0);
        s1 =
            (TE2[(t1 >>> 24)] & 0xff000000) ^
                (TE3[(t2 >>> 16) & 0xff] & 0x00ff0000) ^
                (TE0[(t3 >>> 8) & 0xff] & 0x0000ff00) ^
                (TE1[(t0) & 0xff] & 0x000000ff) ^
                rk[1 + offset];
        putU32(out, 4 + outOff, s1);
        s2 =
            (TE2[(t2 >>> 24)] & 0xff000000) ^
                (TE3[(t3 >>> 16) & 0xff] & 0x00ff0000) ^
                (TE0[(t0 >>> 8) & 0xff] & 0x0000ff00) ^
                (TE1[(t1) & 0xff] & 0x000000ff) ^
                rk[2 + offset];
        putU32(out, 8 + outOff, s2);
        s3 =
            (TE2[(t3 >>> 24)] & 0xff000000) ^
                (TE3[(t0 >>> 16) & 0xff] & 0x00ff0000) ^
                (TE0[(t1 >>> 8) & 0xff] & 0x0000ff00) ^
                (TE1[(t2) & 0xff] & 0x000000ff) ^
                rk[3 + offset];
        putU32(out, 12 + outOff, s3);
    }

    protected static void decryptSingleBlock(byte[] in, int inOff,
                                             byte[] out, int outOff,
                                             EncryptionKey encryptionKey) {
        int[] rk = encryptionKey.rk;
        int s0 = getU32(in, 0 + inOff) ^ rk[0];
        int s1 = getU32(in, 4 + inOff) ^ rk[1];
        int s2 = getU32(in, 8 + inOff) ^ rk[2];
        int s3 = getU32(in, 12 + inOff) ^ rk[3];
        int t0, t1, t2, t3;
        int r = encryptionKey.round >>> 1;
        int offset = 0;
        for (; ; ) {
            t0 =
                TD0[(s0 >>> 24)] ^
                    TD1[(s3 >>> 16) & 0xff] ^
                    Td2[(s2 >>> 8) & 0xff] ^
                    Td3[(s1) & 0xff] ^
                    rk[4 + offset];
            t1 =
                TD0[(s1 >>> 24)] ^
                    TD1[(s0 >>> 16) & 0xff] ^
                    Td2[(s3 >>> 8) & 0xff] ^
                    Td3[(s2) & 0xff] ^
                    rk[5 + offset];
            t2 =
                TD0[(s2 >>> 24)] ^
                    TD1[(s1 >>> 16) & 0xff] ^
                    Td2[(s0 >>> 8) & 0xff] ^
                    Td3[(s3) & 0xff] ^
                    rk[6 + offset];
            t3 =
                TD0[(s3 >>> 24)] ^
                    TD1[(s2 >>> 16) & 0xff] ^
                    Td2[(s1 >>> 8) & 0xff] ^
                    Td3[(s0) & 0xff] ^
                    rk[7 + offset];

            offset += 8;
            if (--r == 0) {
                break;
            }

            s0 =
                TD0[(t0 >>> 24)] ^
                    TD1[(t3 >>> 16) & 0xff] ^
                    Td2[(t2 >>> 8) & 0xff] ^
                    Td3[(t1) & 0xff] ^
                    rk[0 + offset];
            s1 =
                TD0[(t1 >>> 24)] ^
                    TD1[(t0 >>> 16) & 0xff] ^
                    Td2[(t3 >>> 8) & 0xff] ^
                    Td3[(t2) & 0xff] ^
                    rk[1 + offset];
            s2 =
                TD0[(t2 >>> 24)] ^
                    TD1[(t1 >>> 16) & 0xff] ^
                    Td2[(t0 >>> 8) & 0xff] ^
                    Td3[(t3) & 0xff] ^
                    rk[2 + offset];
            s3 =
                TD0[(t3 >>> 24)] ^
                    TD1[(t2 >>> 16) & 0xff] ^
                    Td2[(t1 >>> 8) & 0xff] ^
                    Td3[(t0) & 0xff] ^
                    rk[3 + offset];
        }

        s0 =
            (Td4[(t0 >>> 24)] << 24) ^
                (Td4[(t3 >>> 16) & 0xff] << 16) ^
                (Td4[(t2 >>> 8) & 0xff] << 8) ^
                (Td4[(t1) & 0xff]) ^
                rk[0 + offset];
        putU32(out, 0 + outOff, s0);
        s1 =
            (Td4[(t1 >>> 24)] << 24) ^
                (Td4[(t0 >>> 16) & 0xff] << 16) ^
                (Td4[(t3 >>> 8) & 0xff] << 8) ^
                (Td4[(t2) & 0xff]) ^
                rk[1 + offset];
        putU32(out, 4 + outOff, s1);
        s2 =
            (Td4[(t2 >>> 24)] << 24) ^
                (Td4[(t1 >>> 16) & 0xff] << 16) ^
                (Td4[(t0 >>> 8) & 0xff] << 8) ^
                (Td4[(t3) & 0xff]) ^
                rk[2 + offset];
        putU32(out, 8 + outOff, s2);
        s3 =
            (Td4[(t3 >>> 24)] << 24) ^
                (Td4[(t2 >>> 16) & 0xff] << 16) ^
                (Td4[(t1 >>> 8) & 0xff] << 8) ^
                (Td4[(t0) & 0xff]) ^
                rk[3 + offset];
        putU32(out, 12 + outOff, s3);
    }

    protected static void putU32(byte[] target, int offset, int i) {
        target[offset] = (byte) ((i) >>> 24);
        target[offset + 1] = (byte) ((i) >>> 16);
        target[offset + 2] = (byte) ((i) >>> 8);
        target[offset + 3] = (byte) (i);
    }

    protected static int getU32(byte[] userKey, int offset) {
        byte b1 = userKey[offset];
        byte b2 = userKey[offset + 1];
        byte b3 = userKey[offset + 2];
        byte b4 = userKey[offset + 3];

        return ((b1 & 0xFF) << 24) ^ ((b2 & 0xFF) << 16) ^ ((b3 & 0xFF) << 8) ^ (b4 & 0xFF);
    }
}
