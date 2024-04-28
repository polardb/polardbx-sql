package com.alibaba.polardbx.gms.engine;

import java.util.HashMap;
import java.util.Map;

public enum ColdDataStatus {
    IMPLICIT(-1),
    OFF(0),
    ON(1);

    private final int status;

    ColdDataStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    private static Map<Integer, ColdDataStatus> MAP = new HashMap<>();

    static {
        for (ColdDataStatus coldDataStatus : ColdDataStatus.values()) {
            MAP.put(coldDataStatus.getStatus(), coldDataStatus);
        }
    }

    public static ColdDataStatus of(int status) {
        return MAP.get(status);
    }
}
