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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;




public class MD5Utils {

    private static final Log log = LogFactory.getLog(MD5Utils.class);
    private static char[] digits = {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c',
        'd', 'e', 'f'};

    private static Map<Character, Integer> rDigits = new HashMap<Character, Integer>(16);

    static {
        for (int i = 0; i < digits.length; ++i) {
            rDigits.put(digits[i], i);
        }
    }

    private static MD5Utils me = new MD5Utils();
    private MessageDigest mHasher;
    private ReentrantLock opLock = new ReentrantLock();

    private MD5Utils() {
        try {
            mHasher = MessageDigest.getInstance("md5");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static MD5Utils getInstance() {
        return me;
    }

    public String getMD5String(String content) {
        return bytes2string(hash(content));
    }

    public String getMD5String(byte[] content) {
        return bytes2string(hash(content));
    }

    public byte[] getMD5Bytes(byte[] content) {
        return hash(content);
    }

    public byte[] hash(String str) {
        opLock.lock();
        try {
            byte[] bt = mHasher.digest(str.getBytes("UTF-8"));
            if (null == bt || bt.length != 16) {
                throw new IllegalArgumentException("md5 need");
            }
            return bt;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("unsupported utf-8 encoding", e);
        } finally {
            opLock.unlock();
        }
    }

    public byte[] hash(byte[] data) {
        opLock.lock();
        try {
            byte[] bt = mHasher.digest(data);
            if (null == bt || bt.length != 16) {
                throw new IllegalArgumentException("md5 need");
            }
            return bt;
        } finally {
            opLock.unlock();
        }
    }

    public String bytes2string(byte[] bt) {
        int l = bt.length;

        char[] out = new char[l << 1];

        for (int i = 0, j = 0; i < l; i++) {
            out[j++] = digits[(0xF0 & bt[i]) >>> 4];
            out[j++] = digits[0x0F & bt[i]];
        }

        if (log.isDebugEnabled()) {
            log.debug("[hash]" + (new String(out)));
        }

        return new String(out);
    }

    public byte[] string2bytes(String str) {
        if (null == str) {
            throw new IllegalArgumentException("str is null");
        }
        if (str.length() != 32) {
            throw new IllegalArgumentException("str.length() != 32");
        }

        byte[] data = new byte[16];
        char[] chs = str.toCharArray();
        for (int i = 0; i < 16; ++i) {
            int h = rDigits.get(chs[i * 2]).intValue();
            int l = rDigits.get(chs[i * 2 + 1]).intValue();
            data[i] = (byte) ((h & 0x0F) << 4 | (l & 0x0F));
        }
        return data;
    }

}
