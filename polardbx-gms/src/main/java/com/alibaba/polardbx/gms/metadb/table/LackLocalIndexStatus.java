package com.alibaba.polardbx.gms.metadb.table;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public enum LackLocalIndexStatus {
    NO_LACKIING(0L),
    LACKING(1L);

    private final long value;

    LackLocalIndexStatus(long value) {
        this.value = value;
    }

    public long getValue() {
        return this.value;
    }

    public static LackLocalIndexStatus convert(Boolean value) {
        if (value == null) {
            return NO_LACKIING;
        } else if (!value) {
            return NO_LACKIING;
        } else {
            return LACKING;
        }
    }

    public static LackLocalIndexStatus convert(Long value) {
        if (value == null) {
            return NO_LACKIING;
        } else if (value == 0) {
            return NO_LACKIING;
        } else {
            return LACKING;
        }
    }
}
