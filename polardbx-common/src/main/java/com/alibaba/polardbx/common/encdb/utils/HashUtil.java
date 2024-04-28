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
