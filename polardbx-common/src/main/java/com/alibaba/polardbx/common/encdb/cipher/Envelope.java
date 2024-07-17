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

import com.alibaba.polardbx.common.encdb.enums.AsymmAlgo;
import com.google.common.primitives.Bytes;
import org.bouncycastle.crypto.CryptoException;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

public class Envelope {
    byte[] encryptedKey;
    byte[] data; //AES-128-CBC wrapped, for encrypted data, this also including IV and padding
    CipherSuite cipherSuite;

    public Envelope(byte[] data) {
        this.data = data;
    }

    public byte[] getBytes() throws DataFormatException {
        /*2 bytes short to store the encryptedKey length*/
        int bufSize = 2 + encryptedKey.length + data.length;
        ByteBuffer buf = ByteBuffer.allocate(bufSize).order(ByteOrder.LITTLE_ENDIAN);

        buf.putShort((short) encryptedKey.length);
        buf.put(encryptedKey);
        buf.put(data);

        return buf.array();
    }

    public static Envelope fromBytes(byte[] bytes) throws DataFormatException {
        try {
            ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            Envelope env = new Envelope(null);

            short encryptedKeyLen = buf.getShort();
            env.encryptedKey = new byte[encryptedKeyLen];
            buf.get(env.encryptedKey);

            int dataLen = buf.remaining();
            env.data = new byte[dataLen];
            buf.get(env.data);

            return env;
        } catch (BufferUnderflowException e) {
            throw new DataFormatException("Wrong encdb envelope bytes");
        }
    }

    public Envelope seal(String pukString, boolean usePemSMx) throws CryptoException, IOException {
        SecureRandom sr = new SecureRandom();
        byte[] tempKey = null, iv = null, tempData = null;
        List<Byte> encryptedDataBlob = new ArrayList<>();

        assert cipherSuite != null;

        if (cipherSuite.getAsymmAlgo() == AsymmAlgo.SM2 && cipherSuite.getSymmAlgo().name().startsWith("SM4")) {
            tempKey = new byte[SymCrypto.SM4_KEY_SIZE];
            iv = new byte[SymCrypto.CBCIVLength];
            sr.nextBytes(tempKey);
            sr.nextBytes(iv);

            encryptedDataBlob.addAll(Bytes.asList(iv));
            tempData = SymCrypto.sm4CBCEncrypt(tempKey, data, iv);
            encryptedDataBlob.addAll(Bytes.asList(tempData));
            data = Bytes.toArray(encryptedDataBlob);
            encryptedKey = usePemSMx ? AsymCrypto.sm2EncryptPem(pukString, tempKey)
                : AsymCrypto.sm2EncryptRaw(pukString, tempKey);

        } else if (cipherSuite.getAsymmAlgo() == AsymmAlgo.RSA && cipherSuite.getSymmAlgo().name().startsWith("AES")) {
            tempKey = new byte[SymCrypto.AES_128_KEY_SIZE];
            iv = new byte[SymCrypto.CBCIVLength];
            sr.nextBytes(tempKey);
            sr.nextBytes(iv);

            encryptedDataBlob.addAll(Bytes.asList(iv));
            tempData = SymCrypto.aesCBCEncrypt(tempKey, data, iv);
            encryptedDataBlob.addAll(Bytes.asList(tempData));
            data = Bytes.toArray(encryptedDataBlob);

            encryptedKey = AsymCrypto.rsaPKCS1EncryptPem(pukString, tempKey);
        } else {
            throw new CryptoException("Not supported seal algorithm");
        }

        return this;
    }

    public Envelope seal(String pukString) throws CryptoException, IOException {
        return seal(pukString, pukString.startsWith("-----BEGIN"));
    }

    public byte[] open(String priString)
        throws CryptoException, NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        return open(priString, priString.startsWith("-----BEGIN"));
    }

    public byte[] open(String priString, boolean usePemSMx)
        throws CryptoException, NoSuchAlgorithmException, InvalidKeySpecException, IOException {
        byte[] plaintext = null;
        byte[] tempKey = null, iv = null, tempData = null;
        List<Byte> encryptedDataBlob = Bytes.asList(data);

        int idx = 0;
        iv = Bytes.toArray(encryptedDataBlob.subList(0, idx + SymCrypto.CBCIVLength));
        tempData = Bytes.toArray(encryptedDataBlob.subList(SymCrypto.CBCIVLength, encryptedDataBlob.size()));

        assert cipherSuite != null;

        if (cipherSuite.getAsymmAlgo() == AsymmAlgo.SM2 && cipherSuite.getSymmAlgo().name().startsWith("SM4")) {
            tempKey = usePemSMx ? AsymCrypto.sm2DecryptPem(priString, encryptedKey)
                : AsymCrypto.sm2DecryptRaw(priString, encryptedKey);
            plaintext = SymCrypto.sm4CBCDecrypt(tempKey, tempData, iv);
        } else if (cipherSuite.getAsymmAlgo() == AsymmAlgo.RSA && cipherSuite.getSymmAlgo().name().startsWith("AES")) {
            tempKey = AsymCrypto.rsaPKCS1DecryptPem(priString, encryptedKey);
            plaintext = SymCrypto.aesCBCDecrypt(tempKey, tempData, iv);
        } else {
            throw new CryptoException("Not supported seal algorithm");
        }

        return plaintext;
    }

    public Envelope setCiperSuite(CipherSuite cipherSuite) {
        this.cipherSuite = cipherSuite;
        return this;
    }
}
