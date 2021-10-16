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

import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SecurityUtil {

    private static final int CACHING_SHA2_DIGEST_LENGTH = 32;

    public static byte[] calcMysqlUserPassword(byte[] plainPassword) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] pass1 = md.digest(plainPassword);
        md.reset();
        return md.digest(pass1);
    }

    public static boolean verify(byte[] token, byte[] mysqlUserPassword, byte[] scramble)
        throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        md.update(scramble);
        byte[] stage1_hash = md.digest(mysqlUserPassword);
        for (int i = 0; i < stage1_hash.length; i++) {
            stage1_hash[i] = (byte) (stage1_hash[i] ^ token[i]);
        }

        md.reset();
        byte[] candidate_hash2 = md.digest(stage1_hash);
        boolean match = true;

        if (mysqlUserPassword.length != candidate_hash2.length) {
            match = false;
        }

        for (int i = 0; i < candidate_hash2.length; i++) {
            if (candidate_hash2[i] != mysqlUserPassword[i]) {
                match = false;
                break;
            }
        }

        return match;
    }

    public static final byte[] sha1Pass(byte[] pass) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        return md.digest(pass);
    }

    public static byte[] scrambleCachingSha2(byte[] password, byte[] seed) throws DigestException {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ex) {
            throw new DigestException(ex);
        }

        byte[] dig1 = new byte[CACHING_SHA2_DIGEST_LENGTH];
        byte[] dig2 = new byte[CACHING_SHA2_DIGEST_LENGTH];
        byte[] scramble1 = new byte[CACHING_SHA2_DIGEST_LENGTH];

        md.update(password, 0, password.length);
        md.digest(dig1, 0, CACHING_SHA2_DIGEST_LENGTH);
        md.reset();

        md.update(dig1, 0, dig1.length);
        md.digest(dig2, 0, CACHING_SHA2_DIGEST_LENGTH);
        md.reset();

        md.update(dig2, 0, dig1.length);
        md.update(seed, 0, seed.length);
        md.digest(scramble1, 0, CACHING_SHA2_DIGEST_LENGTH);

        byte[] mysqlScrambleBuff = new byte[CACHING_SHA2_DIGEST_LENGTH];
        xorString(dig1, mysqlScrambleBuff, scramble1, CACHING_SHA2_DIGEST_LENGTH);

        return mysqlScrambleBuff;
    }

    public static final byte[] scramble411BySha1Pass(byte[] pass, byte[] seed) throws NoSuchAlgorithmException {

        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] pass1 = md.digest(pass);
        md.reset();
        md.update(seed);
        byte[] pass2 = md.digest(pass1);
        for (int i = 0; i < pass2.length; i++) {
            pass2[i] = (byte) (pass2[i] ^ pass[i]);
        }
        return pass2;
    }

    public static final byte[] scramble411(byte[] pass, byte[] seed) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] pass1 = md.digest(pass);
        md.reset();
        byte[] pass2 = md.digest(pass1);
        md.reset();
        md.update(seed);
        byte[] pass3 = md.digest(pass2);
        for (int i = 0; i < pass3.length; i++) {
            pass3[i] = (byte) (pass3[i] ^ pass1[i]);
        }
        return pass3;
    }

    public static final String scramble323(String pass, String seed) {
        if ((pass == null) || (pass.length() == 0)) {
            return pass;
        }
        byte b;
        double d;
        long[] pw = hash(seed);
        long[] msg = hash(pass);
        long max = 0x3fffffffL;
        long seed1 = (pw[0] ^ msg[0]) % max;
        long seed2 = (pw[1] ^ msg[1]) % max;
        char[] chars = new char[seed.length()];
        for (int i = 0; i < seed.length(); i++) {
            seed1 = ((seed1 * 3) + seed2) % max;
            seed2 = (seed1 + seed2 + 33) % max;
            d = (double) seed1 / (double) max;
            b = (byte) java.lang.Math.floor((d * 31) + 64);
            chars[i] = (char) b;
        }
        seed1 = ((seed1 * 3) + seed2) % max;
        seed2 = (seed1 + seed2 + 33) % max;
        d = (double) seed1 / (double) max;
        b = (byte) java.lang.Math.floor(d * 31);
        for (int i = 0; i < seed.length(); i++) {
            chars[i] ^= (char) b;
        }
        return new String(chars);
    }

    private static long[] hash(String src) {
        long nr = 1345345333L;
        long add = 7;
        long nr2 = 0x12345671L;
        long tmp;
        for (int i = 0; i < src.length(); ++i) {
            switch (src.charAt(i)) {
            case ' ':
            case '\t':
                continue;
            default:
                tmp = (0xff & src.charAt(i));
                nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
                nr2 += ((nr2 << 8) ^ nr);
                add += tmp;
            }
        }
        long[] result = new long[2];
        result[0] = nr & 0x7fffffffL;
        result[1] = nr2 & 0x7fffffffL;
        return result;
    }

    public static String byte2HexStr(byte[] b) {
        StringBuilder hs = new StringBuilder();
        for (int n = 0; n < b.length; n++) {
            String hex = (Integer.toHexString(b[n] & 0XFF));
            if (hex.length() == 1) {
                hs.append("0" + hex);
            } else {
                hs.append(hex);
            }
        }

        return hs.toString();
    }

    public static byte[] hexStr2Bytes(String src) {
        if (src == null) {
            return null;
        }
        int offset = 0;
        int length = src.length();
        if (length == 0) {
            return new byte[0];
        }

        boolean odd = length << 31 == Integer.MIN_VALUE;
        byte[] bs = new byte[odd ? (length + 1) >> 1 : length >> 1];
        for (int i = offset, limit = offset + length; i < limit; ++i) {
            char high, low;
            if (i == offset && odd) {
                high = '0';
                low = src.charAt(i);
            } else {
                high = src.charAt(i);
                low = src.charAt(++i);
            }
            int b;
            switch (high) {
            case '0':
                b = 0;
                break;
            case '1':
                b = 0x10;
                break;
            case '2':
                b = 0x20;
                break;
            case '3':
                b = 0x30;
                break;
            case '4':
                b = 0x40;
                break;
            case '5':
                b = 0x50;
                break;
            case '6':
                b = 0x60;
                break;
            case '7':
                b = 0x70;
                break;
            case '8':
                b = 0x80;
                break;
            case '9':
                b = 0x90;
                break;
            case 'a':
            case 'A':
                b = 0xa0;
                break;
            case 'b':
            case 'B':
                b = 0xb0;
                break;
            case 'c':
            case 'C':
                b = 0xc0;
                break;
            case 'd':
            case 'D':
                b = 0xd0;
                break;
            case 'e':
            case 'E':
                b = 0xe0;
                break;
            case 'f':
            case 'F':
                b = 0xf0;
                break;
            default:
                throw new IllegalArgumentException("illegal hex-string: " + src);
            }
            switch (low) {
            case '0':
                break;
            case '1':
                b += 1;
                break;
            case '2':
                b += 2;
                break;
            case '3':
                b += 3;
                break;
            case '4':
                b += 4;
                break;
            case '5':
                b += 5;
                break;
            case '6':
                b += 6;
                break;
            case '7':
                b += 7;
                break;
            case '8':
                b += 8;
                break;
            case '9':
                b += 9;
                break;
            case 'a':
            case 'A':
                b += 10;
                break;
            case 'b':
            case 'B':
                b += 11;
                break;
            case 'c':
            case 'C':
                b += 12;
                break;
            case 'd':
            case 'D':
                b += 13;
                break;
            case 'e':
            case 'E':
                b += 14;
                break;
            case 'f':
            case 'F':
                b += 15;
                break;
            default:
                throw new IllegalArgumentException("illegal hex-string: " + src);
            }
            bs[(i - offset) >> 1] = (byte) b;
        }
        return bs;
    }

    private static void xorString(byte[] from, byte[] to, byte[] scramble, int length) {
        int pos = 0;
        int scrambleLength = scramble.length;
        while (pos < length) {
            to[pos] = (byte) (from[pos] ^ scramble[pos % scrambleLength]);
            pos++;
        }
    }
}
