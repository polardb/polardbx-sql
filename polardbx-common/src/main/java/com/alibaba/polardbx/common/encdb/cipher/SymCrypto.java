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

package com.alibaba.polardbx.common.encdb.cipher;

import com.sun.crypto.provider.SunJCE;
import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Objects;

public class SymCrypto {
    public static final int AES_BLOCK_SIZE = 16;
    public static final int AES_128_KEY_SIZE = AES_BLOCK_SIZE;
    public static final int SM4_BLOCK_SIZE = 16;
    public static final int SM4_KEY_SIZE = 16;
    public static final int CLWW_ORE_KEY_SIZE = 32;

    public static final int GCMTagLength = 16;
    public static final int GCMIVLength = 12;
    public static final int CBCIVLength = 16;
    public static final int CTRIVLength = 16;

    static {
        Provider sunJceProvider = new SunJCE();
        if (Security.getProvider(sunJceProvider.getName()) == null) {
            Security.addProvider(sunJceProvider);
        }
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    /**
     *
     */
    private static byte[] gcmEncrypt(byte[] key, byte[] data, byte[] iv, byte[] aad, String algorithm)
        throws CryptoException {
        try {
            SecretKey secretKey = new SecretKeySpec(key, algorithm);
            Cipher cipher = Cipher.getInstance(algorithm + "/GCM/NoPadding");

            if (iv == null) {
                throw new Exception("GCM mode IV should of length " + GCMIVLength);
            }

            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCMTagLength * 8, iv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);
            if (aad != null) {
                cipher.updateAAD(aad);
            }

            return cipher.doFinal(data);
        } catch (Exception e) {
            throw new CryptoException("gcmEncrypt error", e);
        }
    }

    public static byte[] aesGcmEncrypt(byte[] key, byte[] data, byte[] iv, byte[] aad) throws CryptoException {
        return gcmEncrypt(key, data, iv, aad, "AES");
    }

    public static byte[] aesGcmEncrypt(byte[] key, byte[] data, byte[] iv) throws CryptoException {
        return gcmEncrypt(key, data, iv, null, "AES");
    }

    public static byte[] sm4GcmEncrypt(byte[] key, byte[] data, byte[] iv, byte[] aad) throws CryptoException {
        return gcmEncrypt(key, data, iv, aad, "SM4");
    }

    public static byte[] sm4GcmEncrypt(byte[] key, byte[] data, byte[] iv) throws CryptoException {
        return gcmEncrypt(key, data, iv, null, "SM4");
    }

    /*
     * @cipherBytes: cipher data, including tag, in format like cipher || tag, and tag is of length GCMTagLength
     * @iv: iv should of length GCMIVLength if not null
     * @mac: i.e., tag, is of length GCMTagLength
     * @return: data
     */
    private static byte[] gcmDecrypt(byte[] key, byte[] cipherBytes, byte[] iv, byte[] aad, String algorithm)
        throws CryptoException {
        try {
            SecretKey secretKey = new SecretKeySpec(key, algorithm);
            Cipher cipher = Cipher.getInstance(algorithm + "/GCM/NoPadding");

            if (iv.length != GCMIVLength) {
                throw new Exception("GCM mode IV should of length " + GCMIVLength);
            }
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCMTagLength * 8, iv);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, parameterSpec);
            if (aad != null) {
                cipher.updateAAD(aad);
            }

            return cipher.doFinal(cipherBytes);
        } catch (Exception e) {
            throw new CryptoException("gcmDecrypt error", e);
        }
    }

    public static byte[] aesGcmDecrypt(byte[] key, byte[] cipherBytes, byte[] iv, byte[] aad) throws CryptoException {
        return gcmDecrypt(key, cipherBytes, iv, aad, "AES");
    }

    public static byte[] aesGcmDecrypt(byte[] key, byte[] cipherBytes, byte[] iv) throws CryptoException {
        return gcmDecrypt(key, cipherBytes, iv, null, "AES");
    }

    public static byte[] sm4GcmDecrypt(byte[] key, byte[] cipherBytes, byte[] iv, byte[] aad) throws CryptoException {
        return gcmDecrypt(key, cipherBytes, iv, aad, "SM4");
    }

    public static byte[] sm4GcmDecrypt(byte[] key, byte[] cipherBytes, byte[] iv) throws CryptoException {
        return gcmDecrypt(key, cipherBytes, iv, null, "SM4");
    }

    /*
     * @key: symmetric key
     * @data:
     * 		plain data for encryption
     * 		cipher data for decryption
     * @algorithm: AES, SM4, etc.
     * @forEncryption:
     * 		true for encryption
     * 		false for decryption
     */
    private static byte[] ecbPKCS7Cipher(byte[] key, byte[] data, String algorithm, boolean forEncryption)
        throws CryptoException {
        try {
            SecretKeySpec secretKey = new SecretKeySpec(key, algorithm);
            // Note: PKCS5Padding enables SunJCE support. PKCS7Padding leads to BouncyCastle security provider,
            //       which lacks hardware acceleration for AES.
            Cipher cipher = Cipher.getInstance(algorithm + "/ECB/PKCS5Padding");
            cipher.init(forEncryption ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, secretKey);

            return cipher.doFinal(data);
        } catch (Exception e) {
            String errMsg = algorithm + " ecbPKCS7Cipher " + (forEncryption ? " encryption" : " decryption") + " error";
            throw new CryptoException(errMsg, e);
        }
    }

    public static byte[] aesECBEncrypt(byte[] key, byte[] data) throws CryptoException {
        return ecbPKCS7Cipher(key, data, "AES", true);
    }

    public static byte[] aesECBDecrypt(byte[] key, byte[] cipherBytes) throws CryptoException {
        return ecbPKCS7Cipher(key, cipherBytes, "AES", false);
    }

    public static byte[] sm4ECBEncrypt(byte[] key, byte[] data) throws CryptoException {
        return ecbPKCS7Cipher(key, data, "SM4", true);
    }

    public static byte[] sm4ECBDecrypt(byte[] key, byte[] cipherBytes) throws CryptoException {
        return ecbPKCS7Cipher(key, cipherBytes, "SM4", false);
    }

    /*
     * @key: symmetric key
     * @data:
     * 		plain data for encryption
     * 		cipher data for decryption
     * @iv: iv should of length CBCIVLength if not null
     * @algorithm: AES, SM4, etc.
     * @forEncryption:
     * 		true for encryption
     * 		false for decryption
     */
    private static byte[] cbcPKCS7Cipher(byte[] key, byte[] data, byte[] iv, String algorithm, boolean forEncryption)
        throws CryptoException {
        try {
            SecretKeySpec secretKey = new SecretKeySpec(key, algorithm);
            // Note: PKCS5Padding enables SunJCE support. PKCS7Padding leads to BouncyCastle security provider,
            //       which lacks hardware acceleration for AES.
            Cipher cipher = Cipher.getInstance(algorithm + "/CBC/PKCS5Padding");

            if (iv == null && forEncryption) {
                SecureRandom secureRandom = new SecureRandom();
                iv = new byte[CBCIVLength];
                secureRandom.nextBytes(iv);
            }

            if (Objects.requireNonNull(iv).length != CBCIVLength) {
                throw new InvalidAlgorithmParameterException("CBC mode IV should of length " + CBCIVLength);
            }

            IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);
            cipher.init(forEncryption ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, secretKey, ivParameterSpec);

            return cipher.doFinal(data);
        } catch (Exception e) {
            String errMsg = algorithm + " cbcPKCS7Cipher " + (forEncryption ? " encryption" : " decryption") + " error";
            throw new CryptoException(errMsg, e);
        }
    }

    public static byte[] sm4CBCEncrypt(byte[] key, byte[] data, byte[] iv) throws CryptoException {
        return cbcPKCS7Cipher(key, data, iv, "SM4", true);
    }

    public static byte[] sm4CBCDecrypt(byte[] key, byte[] cipherBytes, byte[] iv) throws CryptoException {
        return cbcPKCS7Cipher(key, cipherBytes, iv, "SM4", false);
    }

    public static byte[] aesCBCEncrypt(byte[] key, byte[] data, byte[] iv) throws CryptoException {
        return cbcPKCS7Cipher(key, data, iv, "AES", true);
    }

    public static byte[] aesCBCDecrypt(byte[] key, byte[] cipherBytes, byte[] iv) throws CryptoException {
        return cbcPKCS7Cipher(key, cipherBytes, iv, "AES", false);
    }

    /*
     * @key: symmetric key
     * @data:
     * 		plain data for encryption
     * 		cipher data for decryption
     * @algorithm: AES, SM4, etc.
     * @forEncryption:
     * 		true for encryption
     * 		false for decryption
     */
    private static byte[] ctrCipher(byte[] key, byte[] data, byte[] iv, String algorithm, boolean forEncryption)
        throws CryptoException {
        try {
            SecretKeySpec secretKey = new SecretKeySpec(key, algorithm);
            // Note: PKCS5Padding enables SunJCE support. PKCS7Padding leads to BouncyCastle security provider,
            //       which lacks hardware acceleration for AES.
            Cipher cipher = Cipher.getInstance(algorithm + "/CTR/NoPadding");
            IvParameterSpec ivSpec = new IvParameterSpec(iv);
            cipher.init(forEncryption ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, secretKey, ivSpec);

            return cipher.doFinal(data);
        } catch (Exception e) {
            String errMsg = algorithm + " ctr " + (forEncryption ? " encryption" : " decryption") + " error";
            throw new CryptoException(errMsg, e);
        }
    }

    public static byte[] aesCTRDecrypt(byte[] key, byte[] cipherBytes, byte[] iv) throws CryptoException {
        return ctrCipher(key, cipherBytes, iv, "AES", false);
    }

    public static byte[] sm4CTRDecrypt(byte[] key, byte[] cipherBytes, byte[] iv) throws CryptoException {
        return ctrCipher(key, cipherBytes, iv, "SM4", false);
    }

}
