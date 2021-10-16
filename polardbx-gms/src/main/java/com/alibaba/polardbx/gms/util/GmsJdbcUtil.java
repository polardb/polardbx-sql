/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * @author chenghui.lch
 */
public class GmsJdbcUtil {

    private static final String URL_PATTERN = "jdbc:mysql://%s:%s/%s?%s";
    public static final Map<String, String> JDBC_DEFAULT_CONN_PROPS_MAP =
        new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    static {
        JDBC_DEFAULT_CONN_PROPS_MAP.put("connectTimeout", "10000");
        JDBC_DEFAULT_CONN_PROPS_MAP.put("useUnicode", "true");
        JDBC_DEFAULT_CONN_PROPS_MAP.put("characterEncoding", "utf8");
        JDBC_DEFAULT_CONN_PROPS_MAP.put("autoReconnect", "false");
        JDBC_DEFAULT_CONN_PROPS_MAP.put("failOverReadOnly", "false");
        JDBC_DEFAULT_CONN_PROPS_MAP.put("socketTimeout", "900000");
        JDBC_DEFAULT_CONN_PROPS_MAP.put("rewriteBatchedStatements", "true");
        JDBC_DEFAULT_CONN_PROPS_MAP.put("allowMultiQueries", "true");
        JDBC_DEFAULT_CONN_PROPS_MAP.put("useServerPrepStmts", "false");
        JDBC_DEFAULT_CONN_PROPS_MAP.put("useSSL", "false");
    }

    public static final String DEFAULT_PHY_DB = "mysql";
    public static final String JDBC_SOCKET_TIMEOUT = "socketTimeout";
    public static final String JDBC_CONNECT_TIMEOUT = "connectTimeout";

    public static Map<String, String> getDefaultConnPropertiesForHaChecker() {
        Map<String, String> connPropsMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        connPropsMap.putAll(JDBC_DEFAULT_CONN_PROPS_MAP);
        connPropsMap.put(JDBC_SOCKET_TIMEOUT, "5000");
        return connPropsMap;
    }

    public static Map<String, String> getDefaultConnPropertiesForHaCheckerFast() {
        Map<String, String> connPropsMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        connPropsMap.putAll(JDBC_DEFAULT_CONN_PROPS_MAP);
        connPropsMap.put(JDBC_SOCKET_TIMEOUT, "3000");
        connPropsMap.put(JDBC_CONNECT_TIMEOUT, "3000");
        return connPropsMap;
    }

    public static Map<String, String> getDefaultConnPropertiesForMetaDb() {
        Map<String, String> connPropsMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        connPropsMap.putAll(JDBC_DEFAULT_CONN_PROPS_MAP);
        return connPropsMap;
    }

    public static Map<String, String> getDefaultConnPropertiesForGroup(long socketTimeout) {
        Map<String, String> connPropsMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        connPropsMap.putAll(JDBC_DEFAULT_CONN_PROPS_MAP);
        if (socketTimeout >= 0) {
            connPropsMap.put(JDBC_SOCKET_TIMEOUT, String.valueOf(socketTimeout));
        }
        return connPropsMap;
    }

    public static Connection buildJdbcConnection(String host, int port, String dbName, String user, String passwdEnc,
                                                 String connProps) {
        String passwd = PasswdUtil.decrypt(passwdEnc);
        String url = createUrl(host, port, dbName, connProps);
        Connection conn = createConnection(url, user, passwd);
        return conn;
    }

    public static String createUrl(String host, Integer port, String dbName, String props) {
        String url = String.format(URL_PATTERN, host, port, dbName, props);
        return url;
    }

    public static Connection createConnection(String url, String username, String password) {
        try {
            Connection connection = getConnectionByDriver(username, password, url);
            return connection;
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                String.format("Failed to create connection to [%s]", url));
        }
    }

    public static Connection createConnection(String host, int port, String db, String props, String username,
                                              String password) {
        String url = String.format(URL_PATTERN, host, port, db, props);
        try {
            Connection connection = getConnectionByDriver(username, password, url);
            return connection;
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                String.format("Failed to create connection to [%s], err is %s", url, ex.getMessage()));
        }
    }

    private static Connection getConnectionByDriver(String username, String password, String url) throws SQLException {
        // 1. create driver class
        Driver driver = new com.mysql.jdbc.Driver();

        // 2. prepare driver properties
        Properties info = new Properties();
        info.put("user", username);
        info.put("password", password);

        // 3. use driver to create conn
        return driver.connect(url, info);
    }

    public static Map<String, String> getPropertiesMapFromAtomConnProps(String connProps) {
        return getPropertiesMapFromConnProps(connProps, ";");
    }

    public static String getJdbcConnPropsFromPropertiesMap(Map<String, String> propertiesMap) {
        return getConnPropsFromPropertiesMap(propertiesMap, "&");
    }

    public static String getAtomConnPropsFromPropertiesMap(Map<String, String> propertiesMap) {
        return getConnPropsFromPropertiesMap(propertiesMap, ";");
    }

    public static Map<String, String> getPropertiesMapFromJdbcConnProps(String connProps) {
        return getPropertiesMapFromConnProps(connProps, "&");
    }

    protected static Map<String, String> getPropertiesMapFromConnProps(String connProps, String separator) {
        String[] keyAndValStrArr = connProps.split(separator);
        Map<String, String> keyValMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < keyAndValStrArr.length; i++) {
            String kvStr = keyAndValStrArr[i];
            String[] keyAndVal = kvStr.trim().split("=");
            assert keyAndVal.length == 2;
            String key = keyAndVal[0];
            String val = keyAndVal[1];
            keyValMap.put(key, val);
        }
        return keyValMap;
    }

    protected static String getConnPropsFromPropertiesMap(Map<String, String> propertiesMap, String separator) {
        String connProps = null;
        List<String> keyAndValStrList = new ArrayList<>();
        propertiesMap.forEach((k, v) -> {
            String kvStr = String.format("%s=%s", k, v);
            keyAndValStrList.add(kvStr);
        });
        connProps = StringUtils.join(keyAndValStrList, separator);
        return connProps;
    }
}
