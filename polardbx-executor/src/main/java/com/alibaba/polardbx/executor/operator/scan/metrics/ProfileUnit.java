package com.alibaba.polardbx.executor.operator.scan.metrics;

/**
 * The data unit of metrics.
 */
public enum ProfileUnit {
    NONE(""),
    BYTES("bytes"),
    MILLIS_SECOND("ms"),
    NANO_SECOND("ns");

    private String unitStr;

    ProfileUnit(String unitStr) {
        this.unitStr = unitStr;
    }

    public String getUnitStr() {
        return unitStr;
    }
}
