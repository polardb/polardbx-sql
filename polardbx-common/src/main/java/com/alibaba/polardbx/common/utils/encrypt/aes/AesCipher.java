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
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.modes.CFBBlockCipher;
import org.bouncycastle.crypto.modes.OFBBlockCipher;

public class AesCipher {

    private final static AESEngine AES_ENGINE = new AESEngine();

    private final static BlockCipher CBC_BLOCK_CIPHER
        = new CBCBlockCipher(AES_ENGINE);

    private final static BlockCipher CFB_1_CIPHER = new Cfb1BlockCipher(AES_ENGINE);
    private final static BlockCipher CFB_8_CIPHER = new CFBBlockCipher(AES_ENGINE, 8);
    private final static BlockCipher CFB_128_CIPHER = new CFBBlockCipher(AES_ENGINE, 128);

    private final static BlockCipher OFB_CIPHER = new OFBBlockCipher(AES_ENGINE, 128);

    private final static EcbBlockCipher ECB_CIPHER = new EcbBlockCipher();

    public static BlockCipher getCipher(BlockEncryptionMode encryptionMode) {
        switch (encryptionMode.mode) {
        case CBC:
            return CBC_BLOCK_CIPHER;
        case CFB1:
            return CFB_1_CIPHER;
        case CFB8:
            return CFB_8_CIPHER;
        case CFB128:
            return CFB_128_CIPHER;
        case OFB:
            return OFB_CIPHER;
        case ECB:
        default:
        }
        throw new UnsupportedOperationException("Don't support BlockCipher type "
            + encryptionMode.mode.name());
    }

    public static EcbBlockCipher getEcbCipher() {
        return ECB_CIPHER;
    }
}
