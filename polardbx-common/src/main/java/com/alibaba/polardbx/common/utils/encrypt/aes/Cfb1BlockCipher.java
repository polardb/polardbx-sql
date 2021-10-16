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
import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.DataLengthException;
import org.bouncycastle.crypto.StreamBlockCipher;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;

import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.BLOCK_SIZE;
import static com.alibaba.polardbx.common.utils.encrypt.aes.AesConst.IV_LENGTH;

public class Cfb1BlockCipher extends StreamBlockCipher {
    private final byte[] IV;
    private EncryptionKey encryptionKey;

    private final int blockSize = 1;
    private final BlockCipher cipher;
    private boolean encrypting;

    public Cfb1BlockCipher(BlockCipher cipher) {
        super(cipher);
        this.cipher = cipher;
        this.IV = new byte[IV_LENGTH];
    }

    @Override
    public void init(boolean encrypting, CipherParameters params) throws IllegalArgumentException {
        this.encrypting = encrypting;

        if (params instanceof ParametersWithIV) {
            ParametersWithIV ivParam = (ParametersWithIV) params;
            byte[] iv = ivParam.getIV();
            if (iv.length < IV_LENGTH) {
                throw new IllegalArgumentException("IV length shorter than " + IV_LENGTH);
            } else {
                System.arraycopy(iv, 0, IV, 0, IV.length);
            }
            reset();
            if (ivParam.getParameters() != null) {
                byte[] key = ((KeyParameter) ivParam.getParameters()).getKey();
                this.encryptionKey = new EncryptionKey(key);
                this.encryptionKey.init();
            }
        } else {

            reset();
        }
    }

    @Override
    public int processBytes(byte[] in, int inOff, int len, byte[] out, int outOff)
        throws DataLengthException {

        int totalBits = len * 8;
        byte[] c = new byte[1];
        byte[] tmp = new byte[1];
        for (int n = 0; n < totalBits; n++) {
            byte posBit = (byte) (1 << (7 - n % 8));
            if ((in[inOff + n / 8] & posBit) != 0x00) {
                c[0] = (byte) 0x80;
            } else {
                c[0] = (byte) 0x00;
            }
            doEncryptOrDecrypt(c, tmp, IV);
            out[outOff + n / 8] = (byte) ((out[outOff + n / 8] & (byte) ~posBit) | ((tmp[0] & 0x80) >>> (n % 8)));
        }
        return len;
    }

    private void doEncryptOrDecrypt(final byte[] in, byte[] out, byte[] iv) {
        int n, rem = 1, num = 0;
        byte[] tmpVec = new byte[2 * BLOCK_SIZE + 1];
        System.arraycopy(iv, 0, tmpVec, 0, IV_LENGTH);
        updateIV(iv, iv);

        if (encrypting) {
            for (n = 0; n < 1; ++n) {
                tmpVec[IV_LENGTH + n] = (byte) (in[n] ^ iv[n]);
                out[n] = tmpVec[IV_LENGTH + n];
            }
        } else {
            for (n = 0; n < 1; ++n) {
                tmpVec[16 + n] = in[n];
                out[n] = (byte) (in[n] ^ iv[n]);
            }
        }

        for (n = 0; n < 16; ++n) {
            iv[n] = (byte) ((tmpVec[n + num] & 0xFF) << rem | (tmpVec[n + num + 1] & 0xFF) >>> (8 - rem));
        }
    }

    private void updateIV(byte[] in, byte[] out) {
        AesUtil.encryptSingleBlock(in, 0, out, 0, encryptionKey);
    }

    @Override
    public String getAlgorithmName() {
        return cipher.getAlgorithmName() + "/CFB1";
    }

    @Override
    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public int processBlock(byte[] in, int inOff,
                            byte[] out, int outOff) throws DataLengthException, IllegalStateException {
        processBytes(in, inOff, blockSize, out, outOff);
        return blockSize;
    }

    @Override
    public void reset() {
        cipher.reset();
    }

    @Override
    protected byte calculateByte(byte b) {
        throw new UnsupportedOperationException("Don't support streaming calculate.");
    }
}