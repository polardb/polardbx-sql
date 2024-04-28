package com.alibaba.polardbx.common.encdb.enums;

import java.util.Arrays;

public enum AsymmAlgo implements OrdinalEnum {
    RSA(1),
    SM2(2);

    private final int val;

    AsymmAlgo(int i) {
        this.val = i;
    }

    public static AsymmAlgo from(int i) {
        return Arrays.stream(AsymmAlgo.values())
            .filter(e -> e.val == i)
            .findAny()
            .orElseThrow(() -> new IllegalArgumentException("invalid value"));
    }

    @Override
    public int getVal() {
        return val;
    }
}
