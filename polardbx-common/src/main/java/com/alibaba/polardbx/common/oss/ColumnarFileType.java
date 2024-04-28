package com.alibaba.polardbx.common.oss;

public enum ColumnarFileType {
    ORC, CSV, DEL, SET,
    /**
     * PK IDX log and meta(one record per partition, used as meta lock)
     */
    PK_IDX_LOG,
    PK_IDX_LOG_META,
    PK_IDX_SNAPSHOT,
    PK_IDX_LOCK,
    PK_IDX_SST,
    PK_IDX_BF;

    public static ColumnarFileType of(String fileType) {
        if (fileType == null) {
            return null;
        }

        try {
            return valueOf(fileType);
        } catch (Throwable throwable) {
            try {
                return valueOf(fileType.toUpperCase());
            } catch (Throwable ignorable) {
                return null;
            }
        }
    }

    public boolean isDeltaFile() {
        return this == DEL || this == CSV;
    }
}
