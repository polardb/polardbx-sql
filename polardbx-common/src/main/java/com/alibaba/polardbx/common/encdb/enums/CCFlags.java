package com.alibaba.polardbx.common.encdb.enums;

import java.util.Arrays;

public enum CCFlags implements OrdinalEnum {
    RND(0),
    DET(1);

    private final int val;

    CCFlags(int val) {
        this.val = val;
    }

    @Override
    public int getVal() {
        return val;
    }

    public static CCFlags from(int i) {
        return Arrays.stream(CCFlags.values())
            .filter(e -> e.val == i)
            .findAny()
            .orElseThrow(() -> new IllegalArgumentException("invalid value"));
    }

}
