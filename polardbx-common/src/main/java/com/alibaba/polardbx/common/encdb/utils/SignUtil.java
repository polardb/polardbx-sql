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

import com.alibaba.polardbx.common.encdb.cipher.AsymCrypto;
import com.alibaba.polardbx.common.encdb.enums.AsymmAlgo;
import com.alibaba.polardbx.common.encdb.enums.HashAlgo;
import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.ArrayList;
import java.util.List;

/**
 * 签名工具类
 */
public class SignUtil {

    /**
     * ECDSA公钥验签
     */
    public static boolean verifyWithSha256Ecdsa(PublicKey publicKey, byte[] message, byte[] sign) {
        return verify(publicKey, "SHA256withECDSA", message, sign);
    }

    /**
     * ECDSA私钥签名
     */
    public static byte[] signWithSha256Ecdsa(PrivateKey privateKey, byte[] message) {
        return sign(privateKey, "SHA256withECDSA", message);
    }

    /**
     * 使用RSA PKCS1_5 SHA256验签
     */
    public static boolean verifySha256Rsa(PublicKey publicKey, byte[] message, byte[] sign) {
        return verify(publicKey, "SHA256withRSA", message, sign);
    }

    /**
     * 公钥验签
     */
    public static boolean verify(PublicKey publicKey, String algorithm, byte[] message, byte[] sign) {
        try {
            Signature signature = Signature.getInstance(algorithm);
            signature.initVerify(publicKey);
            signature.update(message);
            return signature.verify(sign);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 使用RSA PKCS1_5 SHA256签名
     */
    public static byte[] signWithSha256Rsa(PrivateKey privateKey, byte[] message) {
        return sign(privateKey, "SHA256withRSA", message);
    }

    /**
     * 私钥签名
     */
    public static byte[] sign(PrivateKey privateKey, String algorithm, byte[] message) {
        try {
            Signature signature = Signature.getInstance(algorithm);
            signature.initSign(privateKey);
            signature.update(message);
            return signature.sign();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // #pragma pack(1)
    // struct HmacBlob {
    //     Hash::Alg alg;     // signature algorithm
    //     uint8_t data[];     // signture value
    // };
    // #pragma pack()
    public static byte[] signHmac(HashAlgo alg, byte[] key, byte[] data) {
        List<Byte> result = new ArrayList<>();
        result.add((byte) alg.getVal());

        byte[] sig = HMAC.hmac(alg, key, data);
        result.addAll(Bytes.asList(sig));

        return Bytes.toArray(result);
    }

    public static boolean verifyHmac(byte[] key, byte[] sigBlob, byte[] data) {
        ByteBuffer bb = ByteBuffer.wrap(sigBlob).order(ByteOrder.LITTLE_ENDIAN);

        HashAlgo alg = HashAlgo.from(bb.get());

        int sigLen = bb.remaining();
        byte[] sig = new byte[sigLen];
        bb.get(sig);

        return signHmac(alg, key, data) == sig;
    }

    // #pragma pack(1)
    // struct SignatureBlob {
    //     Asymmetric::Alg alg;     // signature algorithm
    //     uint8_t data[];     // signture value
    // };
    // #pragma pack()
    public static byte[] sign(AsymmAlgo alg, String privateKeyPemString, byte[] data) throws RuntimeException {
        List<Byte> result = new ArrayList<>();
        result.add((byte) alg.getVal());

        try {
            byte[] sig = AsymCrypto.sign(alg, privateKeyPemString, data);
            result.addAll(Bytes.asList(sig));
        } catch (Exception e) {
            throw new RuntimeException("Sign signature blob fail", e);
        }

        return Bytes.toArray(result);
    }

    public static boolean verify(String publicKeyPemString, byte[] sigBlob, byte[] data) throws RuntimeException {
        ByteBuffer bb = ByteBuffer.wrap(sigBlob).order(ByteOrder.LITTLE_ENDIAN);

        AsymmAlgo alg = AsymmAlgo.from(bb.get());

        int sigLen = bb.remaining();
        byte[] sig = new byte[sigLen];
        bb.get(sig);

        try {
            return AsymCrypto.verify(alg, publicKeyPemString, data, sig);
        } catch (Exception e) {
            throw new RuntimeException("Verify signature blob fail", e);
        }
    }
}
