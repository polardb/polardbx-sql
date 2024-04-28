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

import com.alibaba.polardbx.common.encdb.enums.HashAlgo;
import org.bouncycastle.crypto.digests.SM3Digest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtil {

    /**
     * 计算SHA256，注意不能计算太大对象，因为是全内存的
     *
     * @param rawData raw data
     * @return 长度为32的byte数组
     */
    public static byte[] doSHA256(byte[] rawData) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return digest.digest(rawData);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Panic! hash with SHA256 failed", e);
        }
    }

    public static byte[] doMd5(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");

            return digest.digest(data);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new RuntimeException("Panic! hash with MD5 failed", e);
        }
    }

    /**
     * 返回长度=32的byte数组
     *
     * @param rawData source data
     * @return 长度为32的byte数组
     */
    public static byte[] doSM3(byte[] rawData) {
        SM3Digest digest = new SM3Digest();
        digest.update(rawData, 0, rawData.length);
        byte[] hash = new byte[digest.getDigestSize()];
        digest.doFinal(hash, 0);
        return hash;
    }

    public static byte[] hash(HashAlgo hashAlg, byte[] rawData) {
        switch (hashAlg) {
        case SHA256:
            return doSHA256(rawData);
        case SM3:
            return doSM3(rawData);
        default:
            throw new RuntimeException("Panic! Not support hash algorithm");
        }
    }

    public static String hash(HashAlgo alg, String msg) {
        return Utils.bytesTobase64(HashUtil.hash(alg, msg.getBytes(StandardCharsets.UTF_8)));
    }
}
