package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.utils.TStringUtil;

public enum FileStorageInfoKey {
    ENGINE,
    ENDPOINT,

    FILE_URI,
    FILE_SYSTEM_CONF,

    ACCESS_KEY_ID,
    ACCESS_KEY_SECRET,

    PRIORITY,

    REGION_ID,
    AVAILABLE_ZONE_ID,

    CACHE_POLICY,
    DELETE_POLICY,
    STATUS,

    ENDPOINT_ORDINAL;

    public static FileStorageInfoKey of(String key) {
        if (TStringUtil.isEmpty(key)) {
            return null;
        }
        for (FileStorageInfoKey record : values()) {
            if (record.name().equalsIgnoreCase(key)) {
                return record;
            }
        }
        return null;
    }
}
