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

import org.bouncycastle.crypto.DataLengthException;

import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.BLOCK_SIZE;

public class EcbBlockCipher {

    private EncryptionKey encryptionKey;
    private boolean encrypting;

    public EcbBlockCipher() {

    }

    public void init(boolean encryption, byte[] key) throws IllegalArgumentException {
        this.encrypting = encryption;
        this.encryptionKey = new EncryptionKey(key);
        this.encryptionKey.init();
        if (!encryption) {
            this.encryptionKey.invertKey();
        }
    }

    public byte[] doEncrypt(byte[] plainBytes) throws DataLengthException, IllegalStateException {
        if (!this.encrypting) {

            throw new IllegalStateException("Cannot encrypt in decryption state.");
        }
        int cryptoLen = getResultSize(plainBytes.length);
        byte[] crypto = new byte[cryptoLen];
        int i;
        for (i = 0; i <= plainBytes.length - BLOCK_SIZE; i += BLOCK_SIZE) {
            encryptSingleBlock(plainBytes, i, crypto, i);
        }
        encryptFinal(plainBytes, i, crypto, i);

        return crypto;
    }

    public byte[] doDecrypt(byte[] in) throws DataLengthException, IllegalStateException {
        if (this.encrypting) {

            throw new IllegalStateException("Cannot decrypt in encryption state.");
        }
        checkCryptoSize(in.length);

        byte[] buff = new byte[in.length];
        if (in.length == 0) {
            return buff;
        }
        int i;
        for (i = 0; i < in.length; i += BLOCK_SIZE) {
            decryptSingleBlock(in, i, buff, i);
        }
        int finalLen = decryptFinal(buff, i - BLOCK_SIZE);
        byte[] result = new byte[in.length - BLOCK_SIZE + finalLen];
        System.arraycopy(buff, 0, result, 0, result.length);
        return result;
    }

    private void checkCryptoSize(int length) {
        if (length % BLOCK_SIZE != 0) {
            throw new DataLengthException("Size of crypto for ECB decryption should be a multiple of 16 bytes.");
        }
    }

    private void decryptSingleBlock(byte[] in, int inOff, byte[] out, int outOff) {
        AesUtil.decryptSingleBlock(in, inOff, out, outOff, encryptionKey);
    }

    private int decryptFinal(byte[] in, int inOff) {
        int endPos = inOff + BLOCK_SIZE - 1;
        int padded = in[endPos] & 0xFF;
        if (padded == 0 || padded > BLOCK_SIZE) {
            throw new IllegalStateException("Failed to decrypt in ECB mode: wrong padding.");
        }
        for (int i = 0; i < padded; i++) {
            if (in[endPos - i] != padded) {
                throw new IllegalStateException("Failed to decrypt in ECB mode: wrong padding.");
            }
        }
        return BLOCK_SIZE - padded;
    }

    private void encryptSingleBlock(byte[] in, int inOff, byte[] out, int outOff) {
        AesUtil.encryptSingleBlock(in, inOff, out, outOff, encryptionKey);
    }

    private int getResultSize(int srcLen) {
        return BLOCK_SIZE * (srcLen / BLOCK_SIZE + 1);
    }

    private void encryptFinal(byte[] in, int inOff, byte[] out, int outOff) {
        int remain = in.length - inOff;
        if (remain == BLOCK_SIZE) {
            encryptSingleBlock(in, inOff, out, outOff);
            return;
        }
        int toPad = BLOCK_SIZE - remain;
        byte[] buff = new byte[BLOCK_SIZE];
        System.arraycopy(in, inOff, buff, 0, remain);

        for (int i = 0; i < toPad; i++) {
            buff[remain + i] = (byte) toPad;
        }
        encryptSingleBlock(buff, 0, out, outOff);
    }
}