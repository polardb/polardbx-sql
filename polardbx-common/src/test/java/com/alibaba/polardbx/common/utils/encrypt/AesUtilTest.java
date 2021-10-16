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

package com.alibaba.polardbx.common.utils.encrypt;

import com.alibaba.polardbx.common.utils.encrypt.aes.AesUtil;
import com.alibaba.polardbx.common.utils.encrypt.aes.BlockEncryptionMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class AesUtilTest {

    @Test
    public void testAes() throws Exception {
        final int[] keylens = {128, 192, 256};
        final String[] algorithms = {"ecb", "cbc", "cfb1", "cfb8", "cfb128", "ofb"};

        final String plainText = "ZqEDZ4vsRNmfex0";
        final String key = "key";
        for (int keylen : keylens) {
            for (String algo : algorithms) {
                String mode = String.format("aes-%d-%s", keylen, algo);
                BlockEncryptionMode encryptionMode = new BlockEncryptionMode(mode, true);

                Random random = new Random();
                byte[] iv = new byte[16];
                random.nextBytes(iv);

                byte[] crypto = AesUtil.encryptToBytes(encryptionMode, plainText.getBytes(),
                    key.getBytes(), iv);
                byte[] decryptedResult = AesUtil.decryptToBytes(encryptionMode, crypto,
                    key.getBytes(), iv);
                String decryptedStr = new String(decryptedResult);
                Assert.assertEquals("Failed in mode " + mode, decryptedStr, plainText);
            }
        }

    }
}
