package com.alibaba.polardbx.gms.config;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SqlEngineAlert {
    public static final String NORMAL = "sql_engine_alert_normal";
    public static final String MAJOR = "sql_engine_alert_major";
    public static final String CRITICAL = "sql_engine_alert_critical";

    private static final int DETAIL_MAX_LEN = 200;
    private volatile Map<String, String> alterMap;

    private static final SqlEngineAlert INSTANCE = new SqlEngineAlert();

    public static SqlEngineAlert getInstance() {
        return INSTANCE;
    }

    public SqlEngineAlert() {
        this.alterMap = new ConcurrentHashMap<>();
    }

    public void put(String event, String detail) {
        if (!InstConfUtil.getBool(ConnectionParams.ENABLE_SQL_ENGINE_ALERT)) {
            return;
        }
        if (StringUtils.isNotEmpty(event) && StringUtils.isNotEmpty(detail)) {
            this.alterMap.put(event.toLowerCase(), detail.length() < DETAIL_MAX_LEN ?
                detail : detail.substring(0, DETAIL_MAX_LEN));
        }
    }

    public void putNormal(String detail) {
        if (InstConfUtil.getBool(ConnectionParams.ENABLE_SQL_ENGINE_ALERT_NORMAL)) {
            SqlEngineAlert.getInstance().put(NORMAL, detail);
        }
    }

    public void putMajor(String detail) {
        if (InstConfUtil.getBool(ConnectionParams.ENABLE_SQL_ENGINE_ALERT_MAJOR)) {
            SqlEngineAlert.getInstance().put(MAJOR, detail);
        }
    }

    public void putCritical(String detail) {
        if (InstConfUtil.getBool(ConnectionParams.ENABLE_SQL_ENGINE_ALERT_CRITICAL)) {
            SqlEngineAlert.getInstance().put(CRITICAL, detail);
        }
    }

    public Map<String, String> collectAndClear() {
        Map<String, String> result = this.alterMap;
        this.alterMap = new ConcurrentHashMap<>();
        return result;
    }
}
