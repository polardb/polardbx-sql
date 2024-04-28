package com.alibaba.polardbx.executor.operator.scan;

/**
 * Policy of data scan.
 */
public enum ScanPolicy {
    IO_PRIORITY(1),

    FILTER_PRIORITY(2),

    MERGE_IO(3),

    DELETED_SCAN(4);

    private final int policyId;

    ScanPolicy(int policyId) {
        this.policyId = policyId;
    }

    public int getPolicyId() {
        return policyId;
    }

    public static ScanPolicy of(final int policyId) {
        switch (policyId) {
        case 4:
            return DELETED_SCAN;
        case 2:
            return FILTER_PRIORITY;
        case 3:
            return MERGE_IO;
        case 1:
        default:
            return IO_PRIORITY;
        }
    }
}
