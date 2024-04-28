package com.alibaba.polardbx.common.encdb.enums;

import java.util.Arrays;

public enum HashAlgo implements OrdinalEnum {
    // MUST match that in encdb::Hash::Alg
    SHA256(0),
    SM3(1);

    private final int val;

    HashAlgo(int i) {
        this.val = i;
    }

    public static HashAlgo from(int i) {
        return Arrays.stream(HashAlgo.values())
            .filter(e -> e.val == i)
            .findAny()
            .orElseThrow(() -> new IllegalArgumentException("invalid value"));
    }

    @Override
    public int getVal() {
        return val;
    }
}
