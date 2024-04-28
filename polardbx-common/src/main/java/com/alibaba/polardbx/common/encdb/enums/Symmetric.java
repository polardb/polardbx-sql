package com.alibaba.polardbx.common.encdb.enums;

import java.util.Arrays;

public class Symmetric {

    /**
     * Algorithm parameters
     */
    public enum Params implements OrdinalEnum {
        AES_BLOCK_SIZE(16),
        SM4_BLOCK_SIZE(16),
        MAX_BLOCK_SIZE(16),

        AES_128_KEY_SIZE(16),
        SM4_128_KEY_SIZE(16),
        MAX_KEY_SIZE(16),

        GCM_IV_SIZE(12),
        CBC_IV_SIZE(16),
        CTR_IV_SIZE(16),
        ECB_IV_SIZE(0),
        MAX_IV_SIZE(16),

        GCM_TAG_SIZE(16);

        private final int val;

        Params(int val) {
            this.val = val;
        }

        @Override
        public int getVal() {
            return val;
        }

        public static Params from(int i) {
            return Arrays.stream(Params.values())
                .filter(e -> e.val == i)
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("invalid value"));
        }
    }

}
