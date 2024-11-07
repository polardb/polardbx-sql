package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.druid.util.StringUtils;

/**
 * @author yuehan.wcf
 */

public enum RangeScanMode {
    SERIALIZE,

    NORMAL,

    ADAPTIVE;

    /**
     * Retrieves the corresponding RangeScanMode enumeration value based on the input string parameter 'mode'.
     *
     * @param mode A string parameter representing the scan mode, with possible values being "serialize", "normal", or "adaptive", case-insensitive.
     * @return The corresponding RangeScanMode enumeration value. Returns null if the input mode is empty, null, or does not match any enumeration value.
     */
    public static RangeScanMode getMode(String mode) {
        // Check if the input mode is empty
        if (StringUtils.isEmpty(mode)) {
            return null;
        }

        // Convert mode to lowercase and trim leading/trailing whitespace for uniform comparison
        mode = mode.toLowerCase().trim();
        switch (mode) {
        case "serialize":
            return SERIALIZE;
        case "normal":
            return NORMAL;
        case "adaptive":
            return ADAPTIVE;
        default:
            return null;
        }
    }

    public boolean isNormalMode() {
        return this == NORMAL;
    }
}
