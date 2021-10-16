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

import org.apache.commons.lang.StringUtils;

public class BlockEncryptionMode {
    public static final String AES_PREFIX = "aes";

    public static final BlockEncryptionMode DEFAULT_MODE = new BlockEncryptionMode(128, Mode.ECB);

    int keylen;
    Mode mode;

    public enum Mode {
        ECB("ecb", false),
        CBC("cbc", true),
        CFB1("cfb1", true),
        CFB8("cfb8", true),
        CFB128("cfb128", true),
        OFB("ofb", true),

        OTHER("other", false);
        private final String name;

        final boolean initVectorRequired;

        Mode(String name, boolean initVectorRequired) {
            this.name = name;
            this.initVectorRequired = initVectorRequired;
        }

        public static Mode fromStringIgnoreCase(String name) {
            for (Mode mode : Mode.values()) {
                if (mode.name.equalsIgnoreCase(name)) {
                    return mode;
                }
            }
            return OTHER;
        }
    }

    public boolean isInitVectorRequired() {
        return mode.initVectorRequired;
    }

    private BlockEncryptionMode(int keylen, Mode mode) {
        this.keylen = keylen;
        this.mode = mode;
    }

    public BlockEncryptionMode(String encryptMode, boolean supportOpenSSL) throws IllegalArgumentException {
        String[] args = encryptMode.split("-");
        if (args.length != 3) {
            illegalBlockEncryptionMode(encryptMode);
        }
        checkEncryptionPrefix(args[0]);
        setKeylen(args[1]);
        setMode(args[2], supportOpenSSL);
    }

    public static BlockEncryptionMode parseMode(String encryptMode) {
        return new BlockEncryptionMode(encryptMode, true);
    }

    private void setMode(String modeStr, boolean supportOpenSSL) {
        Mode mode = Mode.fromStringIgnoreCase(modeStr);
        if (!supportOpenSSL) {
            switch (mode) {
            case CFB1:
            case CFB8:
            case CFB128:
            case OFB:
                throw new NotSupportOpensslException("mode=" + modeStr);
            }
        }
        if (mode == Mode.OTHER) {
            illegalBlockEncryptionMode("mode=" + modeStr);
        }
        this.mode = mode;
    }

    private void setKeylen(String lenStr) {
        try {
            int keylen = Integer.parseInt(lenStr);
            if (!isLegalKeylen(keylen)) {
                illegalBlockEncryptionMode("keylen=" + lenStr);
            }
            this.keylen = keylen;
        } catch (NumberFormatException e) {
            illegalBlockEncryptionMode("keylen=" + lenStr);
        }
    }

    private void checkEncryptionPrefix(String prefix) {
        if (!AES_PREFIX.equalsIgnoreCase(prefix)) {
            illegalBlockEncryptionMode(prefix);
        }
    }

    public String nameWithHyphen() {
        return StringUtils.join(new String[]
            {AES_PREFIX, String.valueOf(keylen), mode.name}, '-');
    }

    private boolean isLegalKeylen(int len) {
        switch (len) {
        case 128:
        case 192:
        case 256:
            return true;
        default:
            return false;
        }
    }

    public int getKeylen() {
        return keylen;
    }

    private void illegalBlockEncryptionMode(String errMsg) throws IllegalArgumentException {
        throw new IllegalArgumentException("wrong block encryption mode: " + errMsg);
    }

    @Override
    public String toString() {
        return nameWithHyphen();
    }

    public static class NotSupportOpensslException extends IllegalArgumentException {
        public NotSupportOpensslException(String s) {
            super("Illegal without OpenSSL support, " + s);
        }
    }
}
