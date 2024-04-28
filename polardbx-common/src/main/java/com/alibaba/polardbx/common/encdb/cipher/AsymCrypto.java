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

import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.AsymmAlgo;
import com.alibaba.polardbx.common.encdb.utils.RSAUtil;
import com.google.common.primitives.Bytes;
import org.bouncycastle.asn1.ASN1EncodableVector;
import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.ASN1OctetString;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.ASN1SequenceParser;
import org.bouncycastle.asn1.BEROctetString;
import org.bouncycastle.asn1.DERSequence;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.engines.SM2Engine;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.crypto.params.ECPrivateKeyParameters;
import org.bouncycastle.crypto.params.ECPublicKeyParameters;
import org.bouncycastle.crypto.params.ParametersWithRandom;
import org.bouncycastle.crypto.signers.SM2Signer;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPrivateKey;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPublicKey;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.math.ec.ECPoint;
import org.bouncycastle.math.ec.custom.gm.SM2P256V1Curve;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.util.encoders.Hex;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigInteger;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.PSSParameterSpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AsymCrypto {
    private final static int MAX_SM2_ENCRYPTION_RETRY = 10;

    static {
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    public static KeyPair generateAsymKeyPair(AsymmAlgo asymmAlgo, int keySize) {
        switch (asymmAlgo) {
        case RSA:
            return generateRsaKeyPair(keySize);
        case SM2:
            return generateSm2KeyPair();
        default:
            throw new EncdbException("unsupported asymmetric algorithm");
        }
    }

    /**
     * SM2算法生成密钥对
     */
    public static KeyPair generateSm2KeyPair() {
        try {
            final ECGenParameterSpec sm2Spec = new ECGenParameterSpec("sm2p256v1");
            // 获取一个椭圆曲线类型的密钥对生成器
            final KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC", new BouncyCastleProvider());
            SecureRandom random = new SecureRandom();
            // 使用SM2的算法区域初始化密钥生成器
            kpg.initialize(sm2Spec, random);
            // 获取密钥对
            KeyPair keyPair = kpg.generateKeyPair();
            return keyPair;
        } catch (Exception e) {
            throw new EncdbException(e);
        }
    }

    /**
     * 生成RSA KeyPair
     */
    public static KeyPair generateRsaKeyPair(int keySize) {
        try {
            KeyPairGenerator kpGen = KeyPairGenerator.getInstance("RSA");
            kpGen.initialize(keySize, new SecureRandom());
            return kpGen.generateKeyPair();
        } catch (Exception e) {
            throw new EncdbException(e);
        }
    }

    private static PrivateKey loadPrivateKey(final Reader reader)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        try (PEMParser pemParser = new PEMParser(reader)) {
            Object readObject = pemParser.readObject();
            while (readObject != null) {
                PrivateKeyInfo privateKeyInfo = getPrivateKeyInfoOrNull(readObject);
                if (privateKeyInfo != null) {
                    return new JcaPEMKeyConverter().getPrivateKey(privateKeyInfo);
                }
                readObject = pemParser.readObject();
            }
        }

        return null;
    }

    /**
     * Find a PrivateKeyInfo in the PEM object details. Returns null if the PEM
     * object type is unknown.
     */
    private static PrivateKeyInfo getPrivateKeyInfoOrNull(Object pemObject) throws NoSuchAlgorithmException {
        PrivateKeyInfo privateKeyInfo = null;
        if (pemObject instanceof PEMKeyPair) {
            PEMKeyPair pemKeyPair = (PEMKeyPair) pemObject;
            privateKeyInfo = pemKeyPair.getPrivateKeyInfo();
        } else if (pemObject instanceof PrivateKeyInfo) {
            privateKeyInfo = (PrivateKeyInfo) pemObject;
        } else {
            System.err.printf("Unknown object '{}' from PEMParser\n", pemObject);
        }

        return privateKeyInfo;
    }

    private static PrivateKey importPrivateKey(final String keypem)
        throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        try (StringReader certReader = new StringReader(keypem);
            BufferedReader reader = new BufferedReader(certReader)) {
            return loadPrivateKey(reader);
        }
    }

    private static PublicKey importPublicKey(final String keyStr) throws IOException {
        try (PEMParser pemParser = new PEMParser(new StringReader(keyStr))) {
            SubjectPublicKeyInfo pkInfo = (SubjectPublicKeyInfo) pemParser.readObject();
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");

            return converter.getPublicKey(pkInfo);
        }
    }

    public static byte[] rsaPKCS1EncryptPem(String pukPemString, byte[] input) throws CryptoException {
        try {
            return RSAUtil.encryptRsaOaepSha256PKCS1(RSAUtil.getPublicKeyPKCS1(pukPemString), input);
        } catch (Exception e) {
            throw new CryptoException("rsaEncryptPem encryption error", e);
        }
    }

    public static byte[] rsaPKCS1DecryptPem(String priPemString, byte[] input) throws CryptoException {
        try {
            PrivateKey privateKey = importPrivateKey(priPemString);
            return RSAUtil.decryptRsaOaepSha256PKCS1(privateKey, input);
        } catch (Exception e) {
            throw new CryptoException("rsaEncryptPem decryption error", e);
        }
    }

    /**
     * use rsaPKCS1EncryptPem instead
     */
    @Deprecated
    public static byte[] rsaPKCS1WrapAesKey(String pukPem, byte[] key) throws CryptoException {
        try {
            PublicKey publicKey = importPublicKey(pukPem);
            Cipher c = Cipher.getInstance("RSA/None/PKCS1Padding");
            SecretKey secretKey = new SecretKeySpec(key, "AES");
            c.init(Cipher.WRAP_MODE, publicKey);
            return c.wrap(secretKey);
        } catch (Exception e) {
            throw new CryptoException("rsaPKCS1WrapAesKey wrap error", e);
        }
    }

    /**
     * use rsaPKCS1DecryptPem instead
     */
    @Deprecated
    public static byte[] rsaPKCS1UnwrapAesKey(String priPem, byte[] wrappedKey) throws CryptoException {
        try {
            PrivateKey privateKey = importPrivateKey(priPem);
            Cipher c = Cipher.getInstance("RSA/None/PKCS1Padding", "BC");
            c.init(Cipher.UNWRAP_MODE, privateKey);
            return c.unwrap(wrappedKey, "AES", Cipher.SECRET_KEY).getEncoded();
        } catch (Exception e) {
            throw new CryptoException("rsaPKCS1UnwrapAesKey unwrap error", e);
        }
    }

    public static byte[] sm2EncryptRaw(byte[] puk, byte[] input) throws InvalidCipherTextException {
        SM2Engine engine = new SM2Engine(SM2Constants.MODE);

        ECPoint point = SM2Constants.CURVE.decodePoint(puk);
        ECPublicKeyParameters ecPukParam = new ECPublicKeyParameters(point, SM2Constants.DOMAIN_PARAMS);

        engine.init(true, new ParametersWithRandom(ecPukParam, new SecureRandom()));
        return engine.processBlock(input, 0, input.length);
    }

    public static byte[] sm2DecryptRaw(byte[] pri, byte[] input) throws InvalidCipherTextException {
        SM2Engine engine = new SM2Engine(SM2Constants.MODE);

        BigInteger d = new BigInteger(1, pri);
        ECPrivateKeyParameters ecPriparam = new ECPrivateKeyParameters(d, SM2Constants.DOMAIN_PARAMS);

        engine.init(false, ecPriparam);
        return engine.processBlock(input, 0, input.length);
    }

    public static byte[] sm2EncryptRaw(String pukString, byte[] input) throws InvalidCipherTextException {
        return sm2EncryptRaw(Hex.decode(pukString), input);
    }

    public static byte[] sm2DecryptRaw(String priString, byte[] input) throws InvalidCipherTextException {
        return sm2DecryptRaw(Hex.decode(priString), input);
    }

    private static byte[] convertSm2CipherFromAsn1Bytes(byte[] asn1Bytes) throws IOException {

        /** SM2 cipher format in bytes sequence:
         *      Compression byte
         *      32 bytes C1.x || 32 bytes C1.y
         *      32 bytes C3
         *      real cipher
         */
        ASN1InputStream asn1InputStream = new ASN1InputStream(asn1Bytes);
        ASN1SequenceParser asn1SequenceParser = ((ASN1Sequence) asn1InputStream.readObject()).parser();
        asn1InputStream.close();

        Byte compression = 0x04;
        ASN1Integer c1_x = (ASN1Integer) asn1SequenceParser.readObject().toASN1Primitive();
        ASN1Integer c1_y = (ASN1Integer) asn1SequenceParser.readObject().toASN1Primitive();
        ASN1OctetString c3 = (ASN1OctetString) asn1SequenceParser.readObject().toASN1Primitive();
        ASN1OctetString cipher = (ASN1OctetString) asn1SequenceParser.readObject().toASN1Primitive();

        List<Byte> sm2Cipher = new ArrayList<>();
        sm2Cipher.add(compression);
        byte[] pointBytes = c1_x.getValue().toByteArray();
        if (pointBytes.length == 33) {
            pointBytes = Arrays.copyOfRange(pointBytes, 1, pointBytes.length);
        }
        sm2Cipher.addAll(Bytes.asList(pointBytes));
        pointBytes = c1_y.getValue().toByteArray();
        if (pointBytes.length == 33) {
            pointBytes = Arrays.copyOfRange(pointBytes, 1, pointBytes.length);
        }
        sm2Cipher.addAll(Bytes.asList(pointBytes));
        sm2Cipher.addAll(Bytes.asList(c3.getOctets()));
        sm2Cipher.addAll(Bytes.asList(cipher.getOctets()));

        return Bytes.toArray(sm2Cipher);
    }

    private static byte[] convertSm2CipherToAsn1Bytes(byte[] data, boolean compression)
        throws IOException, ConvertCipherException {
        /** SM2 cipher format in bytes sequence:
         *      Compression byte
         *      32 bytes C1.x || 32 bytes C1.y
         *      32 bytes C3
         *      real cipher
         */
        byte[] uncompressedBytes = data;
        if (compression) {
            uncompressedBytes = Arrays.copyOfRange(data, 1, data.length);
        }

        BigInteger c1_x = new BigInteger(Arrays.copyOfRange(uncompressedBytes, 0, 32));
        BigInteger c1_y = new BigInteger(Arrays.copyOfRange(uncompressedBytes, 32, 64));
        if (c1_x.toByteArray().length != 32) {
            throw new ConvertCipherException(
                "convert c1_x to BigInteger fail:"
                    + "\nc1_x=" + Hex.toHexString(c1_x.toByteArray())
                    + "\nfrom=" + Hex.toHexString(Arrays.copyOfRange(uncompressedBytes, 0, 32)));
        }
        if (c1_y.toByteArray().length != 32) {
            throw new ConvertCipherException(
                "convert c1_y to BigInteger fail:"
                    + "\nc1_y=" + Hex.toHexString(c1_y.toByteArray())
                    + "\nfrom=" + Hex.toHexString(Arrays.copyOfRange(uncompressedBytes, 32, 64)));
        }

        ASN1OctetString c3 = new BEROctetString(Arrays.copyOfRange(uncompressedBytes, 64, 96));
        ASN1OctetString cipher =
            new BEROctetString(Arrays.copyOfRange(uncompressedBytes, 96, uncompressedBytes.length));

        ASN1EncodableVector encodableVector = new ASN1EncodableVector();
        encodableVector.add(new ASN1Integer(c1_x));
        encodableVector.add(new ASN1Integer(c1_y));
        encodableVector.add(c3);
        encodableVector.add(cipher);
        byte[] asn1Bytes = new DERSequence(encodableVector).getEncoded();

        return asn1Bytes;
    }

    public static byte[] sm2EncryptPem(String pubPemString, byte[] input)
        throws IOException, InvalidCipherTextException {
        PublicKey sm2PublicKey = importPublicKey(pubPemString);
        ECPublicKeyParameters ecPublicKeyParameters =
            new ECPublicKeyParameters(((BCECPublicKey) sm2PublicKey).getQ(), SM2Constants.DOMAIN_PARAMS);
        SM2Engine engine = new SM2Engine(SM2Constants.MODE);

        int i = MAX_SM2_ENCRYPTION_RETRY;
        String exceptionMsg = "";
        while (i-- > 0) {
            try {
                engine.init(true, new ParametersWithRandom(ecPublicKeyParameters, new SecureRandom()));
                byte[] output = engine.processBlock(input, 0, input.length);

                // encode ASN1
                boolean compression = (output.length - input.length) % 2 != 0;

                return convertSm2CipherToAsn1Bytes(output, compression);
            } catch (ConvertCipherException e) {
                exceptionMsg = e.getMessage();
            }
        }
        throw new InvalidCipherTextException(exceptionMsg);
    }

    public static byte[] sm2DecryptPem(String priPemString, byte[] input)
        throws InvalidCipherTextException, NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        PrivateKey sm2PrivateKey = importPrivateKey(priPemString);
        ECPrivateKeyParameters ecPrivateKeyParameters =
            new ECPrivateKeyParameters(((BCECPrivateKey) sm2PrivateKey).getD(), SM2Constants.DOMAIN_PARAMS);
        SM2Engine engine = new SM2Engine(SM2Constants.MODE);
        engine.init(false, ecPrivateKeyParameters);

        byte[] decoded = convertSm2CipherFromAsn1Bytes(input);
        return engine.processBlock(decoded, 0, decoded.length);
    }

    // https://gist.github.com/nielsutrecht/0c1538b22e67c61b890a1b435a22fc99
    // https://github.com/hyperchain/javasdk/blob/master/src/main/java/cn/hyperchain/sdk/crypto/sm/sm2/SM2Util.java
    // https://github.com/bcgit/bc-java/blob/bc3b92f1f0e78b82e2584c5fb4b226a13e7f8b3b/prov/src/test/java/org/bouncycastle/jce/provider/test/SM2SignatureTest.java
    // https://github.com/bcgit/bc-java/blob/bc3b92f1f0e78b82e2584c5fb4b226a13e7f8b3b/core/src/test/java/org/bouncycastle/crypto/test/SM2SignerTest.java
    // https://stackoverflow.com/questions/53728536/how-to-sign-with-rsassa-pss-in-java-correctly
    // https://www.openssl.org/docs/man3.0/man3/EVP_PKEY_CTX_set_rsa_mgf1_md.html
    // https://www.openssl.org/docs/man1.1.1/man3/EVP_DigestSignInit.html

    /**
     * @param algorithm signature algorithm, i.e., SM3withSM2, SHA256withRSA/PSS, etc.
     */
    public static byte[] signRsaWithSha256(String priPemString, byte[] input)
        throws NoSuchAlgorithmException, InvalidKeyException, SignatureException, NoSuchProviderException,
        InvalidKeySpecException, IOException, InvalidAlgorithmParameterException {
        PrivateKey privateKey = importPrivateKey(priPemString);
        Signature signature = Signature.getInstance("SHA256withRSA/PSS", "BC");
        signature.setParameter(new PSSParameterSpec("SHA256", "MGF1", MGF1ParameterSpec.SHA256, 32, 1));
        signature.initSign(privateKey);
        signature.update(input);
        return signature.sign();
    }

    public static boolean verifyRsaWithSha256(String pukPemString, byte[] input, byte[] sig)
        throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeyException, SignatureException, IOException,
        InvalidAlgorithmParameterException {
        PublicKey publicKey = importPublicKey(pukPemString);
        Signature signature = Signature.getInstance("SHA256withRSA/PSS", "BC");
        signature.setParameter(new PSSParameterSpec("SHA256", "MGF1", MGF1ParameterSpec.SHA256, 32, 1));
        signature.initVerify(publicKey);
        signature.update(input);
        return signature.verify(sig);
    }

    public static byte[] sm2SignPem(String priPemString, byte[] input)
        throws NoSuchAlgorithmException, InvalidKeySpecException, IOException, NoSuchProviderException,
        InvalidKeyException, SignatureException, CryptoException {
        PrivateKey sm2PrivateKey = importPrivateKey(priPemString);

        // ECPrivateKeyParameters cbPrivateKeyParameters = new ECPrivateKeyParameters(((BCECPrivateKey)sm2PrivateKey).getD(), SM2Constants.DOMAIN_PARAMS);
        // SM2Signer signer = new SM2Signer();
        // signer.init(true, cbPrivateKeyParameters);
        // signer.update(input, 0, input.length);
        // return signer.generateSignature();

        Signature signature = Signature.getInstance("SM3withSM2", "BC");
        signature.initSign(sm2PrivateKey);
        signature.update(input);
        return signature.sign();
    }

    public static boolean sm2VerifyPem(String pubPemString, byte[] input, byte[] sig)
        throws IOException, InvalidKeyException, SignatureException, NoSuchAlgorithmException, NoSuchProviderException {
        PublicKey sm2PublicKey = importPublicKey(pubPemString);

        // ECPublicKeyParameters ecPublicKeyParameters = new ECPublicKeyParameters(((BCECPublicKey)sm2PublicKey).getQ(), SM2Constants.DOMAIN_PARAMS);
        // SM2Signer signer = new SM2Signer();
        // signer.init(false, ecPublicKeyParameters);
        // signer.update(input, 0, input.length);
        // return signer.verifySignature(sig);

        Signature signature = Signature.getInstance("SM3withSM2", "BC");
        signature.initVerify(sm2PublicKey);
        signature.update(input);
        return signature.verify(sig);
    }

    public static byte[] sm2SignRaw(byte[] pri, byte[] input) throws CryptoException {
        ECPrivateKeyParameters privateKeyParameters =
            new ECPrivateKeyParameters(new BigInteger(1, pri), SM2Constants.DOMAIN_PARAMS);
        // AsymmetricCipherKeyPair asymmetricCipherKeyPair = new AsymmetricCipherKeyPair(null, privateKeyParameters);
        // CipherParameters param = new ParametersWithRandom(asymmetricCipherKeyPair.getPrivate(), new SecureRandom());
        // CipherParameters param = new ParametersWithRandom(privateKeyParameters, new SecureRandom());

        SM2Signer signer = new SM2Signer();
        // signer.init(true, param);
        signer.init(true, privateKeyParameters);
        signer.update(input, 0, input.length);
        return signer.generateSignature();
    }

    public static boolean sm2VerifyRaw(byte[] puk, byte[] input, byte[] sig) {
        ECPoint ecPoint = SM2Constants.CURVE.decodePoint(puk);
        ECPublicKeyParameters ecPublicKeyParameters = new ECPublicKeyParameters(ecPoint, SM2Constants.DOMAIN_PARAMS);

        SM2Signer signer = new SM2Signer();
        signer.init(false, ecPublicKeyParameters);
        signer.update(input, 0, input.length);
        return signer.verifySignature(sig);
    }

    public static byte[] sign(AsymmAlgo alg, String priPemString, byte[] input)
        throws InvalidKeyException, NoSuchAlgorithmException, SignatureException, NoSuchProviderException,
        InvalidKeySpecException, InvalidAlgorithmParameterException, IOException, CryptoException {
        switch (alg) {
        case RSA:
            return signRsaWithSha256(priPemString, input);
        case SM2:
            return sm2SignPem(priPemString, input);
        default:
            throw new RuntimeException("Panic! Not support signature algorithm");
        }
    }

    public static boolean verify(AsymmAlgo alg, String pubPemString, byte[] input, byte[] sig)
        throws InvalidKeyException, NoSuchAlgorithmException, NoSuchProviderException, SignatureException,
        InvalidAlgorithmParameterException, IOException {
        switch (alg) {
        case RSA:
            return verifyRsaWithSha256(pubPemString, input, sig);
        case SM2:
            return sm2VerifyPem(pubPemString, input, sig);
        default:
            throw new RuntimeException("Panic! Not support signature algorithm");
        }
    }

    public static class SM2Constants {
        /*
         * 以下为SM2推荐曲线参数
         */
        public static final SM2P256V1Curve CURVE = new SM2P256V1Curve();
        public final static BigInteger SM2_ECC_P = CURVE.getQ();
        public final static BigInteger SM2_ECC_A = CURVE.getA().toBigInteger();
        public final static BigInteger SM2_ECC_B = CURVE.getB().toBigInteger();
        public final static BigInteger SM2_ECC_N = CURVE.getOrder();
        public final static BigInteger SM2_ECC_H = CURVE.getCofactor();
        public final static BigInteger SM2_ECC_GX = new BigInteger(
            "32C4AE2C1F1981195F9904466A39C9948FE30BBFF2660BE1715A4589334C74C7", 16);
        public final static BigInteger SM2_ECC_GY = new BigInteger(
            "BC3736A2F4F6779C59BDCEE36B692153D0A9877CC62A474002DF32E52139F0A0", 16);
        public static final ECPoint G_POINT = CURVE.createPoint(SM2_ECC_GX, SM2_ECC_GY);
        public static final ECDomainParameters DOMAIN_PARAMS =
            new ECDomainParameters(CURVE, G_POINT, SM2_ECC_N, SM2_ECC_H);

        public static final SM2Engine.Mode MODE = SM2Engine.Mode.C1C3C2;
    }
}

/**
 * ConvertCipherException
 */
class ConvertCipherException extends InvalidCipherTextException {
    public ConvertCipherException() {
    }

    public ConvertCipherException(String message) {
        super(message);
    }

    public ConvertCipherException(String message, Throwable cause) {
        super(message, cause);
    }

}
