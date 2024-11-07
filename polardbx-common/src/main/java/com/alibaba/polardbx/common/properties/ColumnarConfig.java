package com.alibaba.polardbx.common.properties;

import com.alibaba.polardbx.common.columnar.ColumnarOption;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.util.Map;

/**
 * Declare options in ColumnarOptions, and define them in ColumnarConfigHandler.
 * @author yaozhili
 */
public final class ColumnarConfig {
    public static String DEFAULT = "DEFAULT";
    public static String SNAPSHOT = "SNAPSHOT";
    private static Map<String, ColumnarOption> CONFIG = null;

    synchronized public static void init(Map<String, ColumnarOption> config) {
        CONFIG = config;
    }

    public static ColumnarOption get(String k) {
        return null == CONFIG ? null : CONFIG.get(k);
    }

    public static String getValue(String key, Map<String, String> indexConfig, Map<String, String> globalConfig) {
        String val = null;
        if (null != indexConfig) {
            val = indexConfig.get(key);
        }
        if (null == val && null != globalConfig) {
            val = globalConfig.get(key);
        }
        if (null == val && CONFIG.containsKey(key)) {
            val = CONFIG.get(key).getDefault();
        }
        return val;
    }
}
