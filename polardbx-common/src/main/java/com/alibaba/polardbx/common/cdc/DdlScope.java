package com.alibaba.polardbx.common.cdc;

public enum DdlScope {

    Schema(0),

    Instance(1);

    int value;

    DdlScope(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
