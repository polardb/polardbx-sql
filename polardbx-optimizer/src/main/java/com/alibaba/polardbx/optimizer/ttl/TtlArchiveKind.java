package com.alibaba.polardbx.optimizer.ttl;

import com.alibaba.polardbx.druid.util.StringUtils;

/**
 * @author chenhui.lch
 */
public enum TtlArchiveKind {

    UNDEFINED(0, ""),
    COLUMNAR(1, "COLUMNAR"),
    PARTITION(2, "PARTITION"),
    SUBPARTITION(3, "SUBPARTITION");
    private int archiveKindCode;
    private String archiveKindStr;

    TtlArchiveKind(int code, String name) {
        this.archiveKindCode = code;
        this.archiveKindStr = name;
    }

    public static TtlArchiveKind of(Integer archiveKindCode) {
        switch (archiveKindCode) {
        case 0:
            return TtlArchiveKind.UNDEFINED;
        case 1:
            return TtlArchiveKind.COLUMNAR;
        case 2:
            return TtlArchiveKind.PARTITION;
        case 3:
            return TtlArchiveKind.SUBPARTITION;
        case 4:
            return TtlArchiveKind.UNDEFINED;
        }
        return TtlArchiveKind.UNDEFINED;
    }

    public static TtlArchiveKind of(String archiveKindStr) {

        if (StringUtils.isEmpty(archiveKindStr)) {
            return TtlArchiveKind.UNDEFINED;
        }

        if (TtlArchiveKind.UNDEFINED.getArchiveKindStr().equalsIgnoreCase(archiveKindStr)) {
            return TtlArchiveKind.UNDEFINED;
        } else if (TtlArchiveKind.COLUMNAR.getArchiveKindStr().equalsIgnoreCase(archiveKindStr)) {
            return TtlArchiveKind.COLUMNAR;
        } else if (TtlArchiveKind.PARTITION.getArchiveKindStr().equalsIgnoreCase(archiveKindStr)) {
            return TtlArchiveKind.PARTITION;
        } else if (TtlArchiveKind.SUBPARTITION.getArchiveKindStr().equalsIgnoreCase(archiveKindStr)) {
            return TtlArchiveKind.SUBPARTITION;
        } else {
            return TtlArchiveKind.UNDEFINED;
        }
    }

    public int getArchiveKindCode() {
        return archiveKindCode;
    }

    public void setArchiveKindCode(int archiveKindCode) {
        this.archiveKindCode = archiveKindCode;
    }

    public String getArchiveKindStr() {
        return archiveKindStr;
    }

}
