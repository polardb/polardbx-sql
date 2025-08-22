package com.alibaba.polardbx.optimizer.ttl;

import com.alibaba.polardbx.druid.util.StringUtils;

/**
 * @author chenhui.lch
 */
public enum TtlArchivePartMode {

    UNDEFINED(-1, "UNDEFINED"),
    EXPIRE_AFTER_TIME_INTERVAL(0, "EXPIRE_AFTER_TIME_INTERVAL"),
    EXPIRE_OVER_PARTITION_COUNT(1, "EXPIRE_OVER_PARTITION_COUNT");

    private int arcPartModeCode;
    private String arcPartModeNameStr;

    TtlArchivePartMode(int code, String name) {
        this.arcPartModeCode = code;
        this.arcPartModeNameStr = name;
    }

    public static TtlArchivePartMode of(Integer archiveKindCode) {
        switch (archiveKindCode) {
        case 0:
            return TtlArchivePartMode.EXPIRE_AFTER_TIME_INTERVAL;
        case 1:
            return TtlArchivePartMode.EXPIRE_OVER_PARTITION_COUNT;
        default:
            return TtlArchivePartMode.UNDEFINED;
        }
    }

    public static TtlArchivePartMode of(String archiveKindStr) {

        if (StringUtils.isEmpty(archiveKindStr)) {
            return TtlArchivePartMode.UNDEFINED;
        }

        if (TtlArchivePartMode.EXPIRE_AFTER_TIME_INTERVAL.getArcPartModeName().equalsIgnoreCase(archiveKindStr)) {
            return TtlArchivePartMode.EXPIRE_AFTER_TIME_INTERVAL;
        } else if (TtlArchivePartMode.EXPIRE_OVER_PARTITION_COUNT.getArcPartModeName()
            .equalsIgnoreCase(archiveKindStr)) {
            return TtlArchivePartMode.EXPIRE_OVER_PARTITION_COUNT;
        } else {
            return TtlArchivePartMode.UNDEFINED;
        }
    }

    public int getArcPartModeCode() {
        return arcPartModeCode;
    }

    public void setArcPartModeCode(int arcPartModeCode) {
        this.arcPartModeCode = arcPartModeCode;
    }

    public String getArcPartModeName() {
        return arcPartModeNameStr;
    }

    public boolean isExpiredAfterTimeInterval() {
        return this.arcPartModeCode == EXPIRE_AFTER_TIME_INTERVAL.getArcPartModeCode();
    }

    public boolean isExpiredOverPartitionCount() {
        return this.arcPartModeCode == EXPIRE_OVER_PARTITION_COUNT.getArcPartModeCode();
    }

    public boolean isUndefinedExpirePolicy() {
        return this.arcPartModeCode == UNDEFINED.getArcPartModeCode();
    }

}
