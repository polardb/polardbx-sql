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

import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.security.AlgorithmParameters;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class RSAUtil {

    /*jceSecurity will result in OOM if BouncyCastleProvider was not used correctly.
     https://timerbin.iteye.com/blog/2151969
     */
    static {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    public static PublicKey getPublicKey(String base64PublicKey) {
        base64PublicKey = extractPemBlock(base64PublicKey);
        PublicKey publicKey = null;
        try {
            X509EncodedKeySpec keySpec = new X509EncodedKeySpec(Base64.getDecoder().decode(base64PublicKey.getBytes()));
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            publicKey = keyFactory.generatePublic(keySpec);
            return publicKey;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
        }
        return publicKey;
    }

    public static PublicKey getPublicKeyPKCS1(final String keyStr) throws IOException {
        try (PEMParser pemParser = new PEMParser(new StringReader(keyStr))) {
            SubjectPublicKeyInfo pkInfo = (SubjectPublicKeyInfo) pemParser.readObject();
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter()/*.setProvider("BC")*/;

            return converter.getPublicKey(pkInfo);
        }
    }

    public static PrivateKey getPrivateKeyPKCS1(String base64PrivateKey) {
        /*Java does not support load PKCS1 privateKey*/
        PrivateKey privateKey = null;
        try {
            StringReader certReader = new StringReader(base64PrivateKey);
            BufferedReader reader = new BufferedReader(certReader);

            PEMParser pemParser = new PEMParser(reader);
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter()/*.setProvider("BC")*/;
            Object object = pemParser.readObject();
            KeyPair kp = converter.getKeyPair((PEMKeyPair) object);
            privateKey = kp.getPrivate();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return privateKey;
    }

    public static PrivateKey getPrivateKey(String base64PrivateKey) {
        base64PrivateKey = extractPemBlock(base64PrivateKey);
        PrivateKey privateKey = null;
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(base64PrivateKey.getBytes()));
        KeyFactory keyFactory = null;
        try {
            keyFactory = KeyFactory.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        try {
            privateKey = keyFactory.generatePrivate(keySpec);
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
        }
        return privateKey;
    }

    public static byte[] encrypt(String data, String publicKey)
        throws BadPaddingException, IllegalBlockSizeException, InvalidKeyException, NoSuchPaddingException,
        NoSuchAlgorithmException {
        Cipher cipher = Cipher.getInstance("RSA/None/PKCS1Padding");
        cipher.init(Cipher.ENCRYPT_MODE, getPublicKey(publicKey));
        return cipher.doFinal(data.getBytes());
    }

    public static String decrypt(byte[] data, PrivateKey privateKey)
        throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, BadPaddingException,
        IllegalBlockSizeException {
        Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
        cipher.init(Cipher.DECRYPT_MODE, privateKey);
        return new String(cipher.doFinal(data));
    }

    /**
     * 按 `----BEGIN` 和 `-----END` 提取内容
     */
    public static String extractPemBlock(String pem) {
        if (pem == null) {
            throw new IllegalArgumentException("Invalid pem(null)");
        }
        List<String> ret = new ArrayList<>();
        String[] pemLines = pem.split("(\r\n)|(\r)|(\n)");
        boolean isInPem = false;
        for (String pemLine : pemLines) {
            if (isInPem) {
                if (pemLine.trim().startsWith("-----END ")) {
                    break;
                }
                ret.add(pemLine.trim());
            }
            if (pemLine.trim().startsWith("-----BEGIN ")) {
                isInPem = true;
            }
        }

        return String.join("", ret);
//        return StringUtils.join(ret.toArray());
    }

    public static byte[] encrypt256(String data, String publicKey)
        throws BadPaddingException, IllegalBlockSizeException, InvalidKeyException, NoSuchPaddingException,
        NoSuchAlgorithmException {
        Cipher cipher = null;

        try {
            cipher = Cipher.getInstance("RSA/None/PKCS1Padding", "BC");
        } catch (NoSuchProviderException e) {
            e.printStackTrace();
        }

        assert cipher != null;
        cipher.init(Cipher.ENCRYPT_MODE, getPublicKey(publicKey));
        return cipher.doFinal(data.getBytes());
    }

    public static String decrypt(String data, String base64PrivateKey)
        throws IllegalBlockSizeException, InvalidKeyException, BadPaddingException, NoSuchAlgorithmException,
        NoSuchPaddingException {
        return decrypt(Base64.getDecoder().decode(data.getBytes()), getPrivateKey(base64PrivateKey));
    }

    /**
     * 使用 RSA OAEP_SHA256 加密
     *
     * @param publicKey publicKey
     * @param content content
     * @return encrypted bytes
     */
    public static byte[] encryptRsaOaepSha256(PublicKey publicKey, byte[] content) {
        try {
            AlgorithmParameters algp = AlgorithmParameters.getInstance("OAEP");
            AlgorithmParameterSpec paramSpec =
                new OAEPParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256, PSource.PSpecified.DEFAULT);
            algp.init(paramSpec);
            Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
            cipher.init(Cipher.ENCRYPT_MODE, publicKey, algp);
            return cipher.doFinal(content);
        } catch (Exception e) {
            throw new RuntimeException("Encrypt RSA OAEP_SHA256 failed", e);
        }
    }

    /**
     * 使用 RSA OAEP_SHA256 解密
     *
     * @param privateKey privateKey
     * @param cipherText encrypted bytes
     * @return plain bytes
     */
    public static byte[] decryptRsaOaepSha256(PrivateKey privateKey, byte[] cipherText) {
        try {
            AlgorithmParameters algp = AlgorithmParameters.getInstance("OAEP");
            AlgorithmParameterSpec paramSpec =
                new OAEPParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256, PSource.PSpecified.DEFAULT);
            algp.init(paramSpec);
            Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
            cipher.init(Cipher.DECRYPT_MODE, privateKey, algp);
            return cipher.doFinal(cipherText);
        } catch (Exception e) {
            throw new RuntimeException("Decrypt RSA OAEP_SHA256 failed", e);
        }
    }

    public static byte[] encryptRsaOaepSha256PKCS1(PublicKey publicKey, byte[] content) {
        try {
            AlgorithmParameters algp = AlgorithmParameters.getInstance("OAEP");
            AlgorithmParameterSpec paramSpec =
                new OAEPParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256, PSource.PSpecified.DEFAULT);
            algp.init(paramSpec);
            Cipher cipher = Cipher.getInstance("RSA/NONE/OAEPWithSHA-256AndMGF1Padding");
            cipher.init(Cipher.ENCRYPT_MODE, publicKey, algp);
            return cipher.doFinal(content);
        } catch (Exception e) {
            throw new RuntimeException("Encrypt RSA OAEP_SHA256 failed", e);
        }
    }

    public static KeyPair generateRsa2048KeyPair() {
        return generateRsaKeyPair(2048);
    }

    public static KeyPair generateRsa3072KeyPair() {
        return generateRsaKeyPair(3072);
    }

    /**
     * 生成RSA KeyPair
     * keySize:   2048
     */
    public static KeyPair generateRsaKeyPair(int keySize) {
        try {
            KeyPairGenerator kpGen = KeyPairGenerator.getInstance("RSA");
            kpGen.initialize(keySize, new SecureRandom());
            return kpGen.generateKeyPair();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] decryptRsaOaepSha256PKCS1(PrivateKey privateKey, byte[] cipherText) {
        try {
            AlgorithmParameters algp = AlgorithmParameters.getInstance("OAEP");
            AlgorithmParameterSpec paramSpec =
                new OAEPParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256, PSource.PSpecified.DEFAULT);
            algp.init(paramSpec);
            Cipher cipher = Cipher.getInstance("RSA/NONE/OAEPWithSHA-256AndMGF1Padding");
            cipher.init(Cipher.DECRYPT_MODE, privateKey, algp);
            return cipher.doFinal(cipherText);
        } catch (Exception e) {
            throw new RuntimeException("Decrypt RSA OAEP_SHA256 failed", e);
        }
    }

}
