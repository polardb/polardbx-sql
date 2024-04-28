package com.taobao.tddl.common.privilege;

import org.apache.commons.lang3.StringUtils;

/**
 * authentication plugin of user password
 */
public enum AuthPlugin {

    /**
     * 1-round sha-1
     */
    POLARDBX_NATIVE_PASSWORD,
    /**
     * 2-round sha-1
     */
    MYSQL_NATIVE_PASSWORD;

    public static AuthPlugin lookupByName(String pluginName) {
        if (StringUtils.isBlank(pluginName)) {
            return POLARDBX_NATIVE_PASSWORD;
        }

        switch (pluginName.toUpperCase()) {
        case "POLARDBX_NATIVE_PASSWORD":
            return POLARDBX_NATIVE_PASSWORD;
        case "MYSQL_NATIVE_PASSWORD":
            return MYSQL_NATIVE_PASSWORD;
        default:
            throw new RuntimeException("Unsupported auth_plugin: " + pluginName);
        }
    }

    public String toLowerCase() {
        return name().toLowerCase();
    }
}
