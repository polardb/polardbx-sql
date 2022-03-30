package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author chenghui.lch
 */
public class JdbcUtil {

    private static final String URL_PATTERN = "jdbc:mysql://%s:%s/%s?%s";
    public static final String DEFAULT_CONN_PROPS =
        "useUnicode=true&characterEncoding=utf8&useSSL=false&connectTimeout=5000&socketTimeout=12000";
    public static final String DEFAULT_PHY_DB = "mysql";

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
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection(url, username, password);
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                String.format("Failed to create connection to [%s]", url));
        }
    }

    public static Connection createConnection(String host, int port, String db, String props, String username,
                                              String password) {
        String url = String.format(URL_PATTERN, host, port, db, props);
        try {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection(url, username, password);
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                String.format("Failed to create connection to [%s], err is %s", url, ex.getMessage()));
        }
    }

}
