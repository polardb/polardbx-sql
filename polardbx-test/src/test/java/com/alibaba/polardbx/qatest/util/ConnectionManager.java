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

package com.alibaba.polardbx.qatest.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.cdc.CdcTableUtil;
import com.alibaba.polardbx.gms.util.JdbcUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.privileges.encdb.EncdbTestBase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;

import javax.sql.DataSource;
import java.security.Security;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static com.alibaba.polardbx.qatest.util.JdbcUtil.getSqlMode;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.POLARDBX_SERVER_ID_CONF;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.dnCount;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.getConnectionProperties;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.getMetaDB;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.useCursorFetch;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.useDruid;
import static com.alibaba.polardbx.ssl.SslConstant.PROPERTY_TLS_DISABLED_ALGS;

/**
 * 初始化所有连接
 */
public class ConnectionManager {

    private static final Log log = LogFactory.getLog(ConnectionManager.class);

    private static final int MAX_ACTIVE = 60;
    private static ConnectionManager connectionManager = new ConnectionManager();
    private Properties configProp;
    private boolean inited = false;

    private boolean skipInitMysql = false;
    private String mysqlUser;
    private String mysqlPassword;
    private String mysqlPort;
    private String mysqlAddress;

    private String mysqlUserSecond;
    private String mysqlPasswordSecond;
    private String mysqlPortSecond;
    private String mysqlAddressSecond;

    private String polardbxUser;
    private String polardbxPassword;
    private String polardbxPort;
    private String polardbxAddress;

    private String metaUser;
    private String metaPassword;
    private String metaPort;
    private String metaAddress;

    private DruidDataSource mysqlDataSource;
    private DruidDataSource mysqlDataSourceSecond;
    private DruidDataSource metaDataSource;
    private DruidDataSource polardbxDataSource;

    private boolean enableOpenSSL;
    private String polardbxMode;
    private String mysqlMode;

    public static ConnectionManager getInstance() {
        if (!connectionManager.isInited()) {
            synchronized (connectionManager) {
                if (!connectionManager.isInited()) {
                    connectionManager.init();
                }
            }
        }
        return connectionManager;
    }

    public static DruidDataSource getDruidDataSource(String server, String port,
                                                     String user, String password, String db, boolean isMysql) {
        // Do not use MySQL cursor fetch mode to compare, since it has some bug in Date type handling. Using server
        // prepare mode instead.
        String connProp = getConnectionProperties();
        if (useCursorFetch() && isMysql) {
            connProp = connProp.replace("useCursorFetch=true", "");
            connProp = connProp.replace("defaultFetchSize=1", "");
            connProp = connProp + "&useServerPrepStmts=true";
        }
        if (isMysql) {
            connProp = connProp.replace(POLARDBX_SERVER_ID_CONF, "");
        }
        String url = String.format(ConfigConstant.URL_PATTERN_WITH_DB + connProp, server, port,
            db);
        return getDruidDataSource(url, user, password);
    }

    public static DruidDataSource getDruidDataSource(String url, String user, String password) {
        DruidDataSource druidDs = new DruidDataSource();
        druidDs.setUrl(url);
        druidDs.setUsername(user);
        druidDs.setPassword(password);
        druidDs.setRemoveAbandoned(false);
        druidDs.setMaxActive(MAX_ACTIVE);
        try {
            druidDs.init();
            druidDs.getConnection();
        } catch (SQLException e) {
            String errorMs = "[DruidDataSource getConnection] failed! ";
            log.error(errorMs, e);
            Assert.fail(errorMs);
        }
        return druidDs;
    }

    public static Log getLog() {
        return log;
    }

    private void ConnectionManager() {

    }

    private boolean isInited() {
        return inited;
    }

    private void init() {
        //jdk 放开tls限制
        Security.setProperty(PROPERTY_TLS_DISABLED_ALGS, "");

        this.configProp = PropertiesUtil.configProp;

        this.skipInitMysql = Boolean.parseBoolean(configProp.getProperty(ConfigConstant.SKIP_INIT_MYSQL));
        this.mysqlUser = configProp.getProperty(ConfigConstant.MYSQL_USER);
        this.mysqlPassword = configProp.getProperty(ConfigConstant.MYSQL_PASSWORD);
        this.mysqlPort = configProp.getProperty(ConfigConstant.MYSQL_PORT);
        this.mysqlAddress = configProp.getProperty(ConfigConstant.MYSQL_ADDRESS);

        this.mysqlUserSecond = configProp.getProperty(ConfigConstant.MYSQL_USER_SECOND);
        this.mysqlPasswordSecond = configProp.getProperty(ConfigConstant.MYSQL_PASSWORD_SECOND);
        this.mysqlPortSecond = configProp.getProperty(ConfigConstant.MYSQL_PORT_SECOND);
        this.mysqlAddressSecond = configProp.getProperty(ConfigConstant.MYSQL_ADDRESS_SECOND);

        this.polardbxUser = configProp.getProperty(ConfigConstant.POLARDBX_USER);
        this.polardbxPassword = configProp.getProperty(ConfigConstant.POLARDBX_PASSWORD);
        this.polardbxPort = configProp.getProperty(ConfigConstant.POLARDBX_PORT);
        this.polardbxAddress = configProp.getProperty(ConfigConstant.POLARDBX_ADDRESS);

        this.metaUser = configProp.getProperty(ConfigConstant.META_USER);
        this.metaPassword = PasswdUtil.decrypt(configProp.getProperty(ConfigConstant.META_PASSWORD));
        this.metaPort = configProp.getProperty(ConfigConstant.META_PORT);
        this.metaAddress = configProp.getProperty(ConfigConstant.META_ADDRESS);

        try {
            if (!skipInitMysql) {
                this.mysqlDataSource = getDruidDataSource(
                    mysqlAddress, mysqlPort, mysqlUser, mysqlPassword, PropertiesUtil.mysqlDBName1(), true);
                setMysqlParameter(mysqlDataSource);

                if (dnCount > 1) {
                    this.mysqlDataSourceSecond = getDruidDataSource(
                        mysqlAddressSecond, mysqlPortSecond, mysqlUserSecond, mysqlPasswordSecond,
                        PropertiesUtil.mysqlDBName1(), true);
                    setMysqlParameter(mysqlDataSourceSecond);
                }
                try (Connection mysqlCon = mysqlDataSource.getConnection()) {
                    this.enableOpenSSL = checkSupportOpenSSL(mysqlCon);
                    this.mysqlMode = getSqlMode(mysqlCon);
                }
            }

            this.metaDataSource =
                getDruidDataSource(metaAddress, metaPort, metaUser, metaPassword, PropertiesUtil.getMetaDB, true);

            this.polardbxDataSource = getDruidDataSource(polardbxAddress, polardbxPort, polardbxUser, polardbxPassword,
                PropertiesUtil.polardbXDBName1(false), false);

            try (Connection polardbxCon = polardbxDataSource.getConnection()) {
                this.polardbxMode = getSqlMode(polardbxCon);
            }

            try (Connection polardbxCon = polardbxDataSource.getConnection()) {
                com.alibaba.polardbx.qatest.util.JdbcUtil.useDb(polardbxCon, CdcTableUtil.CDC_TABLE_SCHEMA);
                polardbxCon.createStatement().execute(String
                    .format("alter table %s add index idx_job_id(`JOB_ID`)",
                        CdcTableUtil.CDC_DDL_RECORD_TABLE));
            } catch (Throwable t) {
                //ignore
            }

        } catch (Throwable t) {
            log.error(this.toString(), t);
            throw new RuntimeException(t);
        }

        inited = true;
    }

    private void setMysqlParameter(DataSource dataSource) {
        try (Connection mysqlConnection = dataSource.getConnection()) {
            com.alibaba.polardbx.qatest.util.JdbcUtil.executeUpdate(mysqlConnection,
                "set global innodb_buffer_pool_size=6442450944;");
            com.alibaba.polardbx.qatest.util.JdbcUtil.executeUpdate(mysqlConnection,
                "set global table_open_cache=20000;");
            com.alibaba.polardbx.qatest.util.JdbcUtil.executeUpdate(mysqlConnection,
                "set global table_definition_cache=20000;");
            com.alibaba.polardbx.qatest.util.JdbcUtil.executeUpdate(mysqlConnection,
                "set global sync_binlog=1000;");
            com.alibaba.polardbx.qatest.util.JdbcUtil.executeUpdate(mysqlConnection,
                "set global innodb_flush_log_at_trx_commit=2;");
        } catch (Throwable t) {
            //ignore
        }
    }

    public DruidDataSource getMysqlDataSource() {
        return mysqlDataSource;
    }

    public DruidDataSource getMysqlDataSourceSecond() {
        return mysqlDataSourceSecond;
    }

    public DruidDataSource getMetaDataSource() {
        return metaDataSource;
    }

    public DruidDataSource getPolardbxDataSource() {
        return polardbxDataSource;
    }

    public Connection getDruidMysqlConnection() throws SQLException {
        if (useDruid) {
            return getMysqlDataSource().getConnection();
        } else {
            return newMysqlConnection();
        }
    }

    public Connection getDruidMysqlConnectionSecond() throws SQLException {
        if (useDruid) {
            return getMysqlDataSourceSecond().getConnection();
        } else {
            return newMysqlConnectionSecond();
        }
    }

    public Connection getDruidMetaConnection() throws SQLException {
        if (useDruid) {
            return getMetaDataSource().getConnection();
        } else {
            String connProp = getConnectionProperties();
            connProp = connProp.replace(POLARDBX_SERVER_ID_CONF, "");
            String url = String
                .format(ConfigConstant.URL_PATTERN_WITH_DB + connProp, metaAddress, metaPort,
                    getMetaDB);
            return JdbcUtil.createConnection(url, metaUser, metaPassword);
        }
    }

    public Connection getDruidPolardbxConnection() throws SQLException {
        if (useDruid) {
            return getPolardbxDataSource().getConnection();
        } else {
            return newPolarDBXConnection();
        }
    }

    public Connection newPolarDBXConnection() {
        String url =
            String.format(ConfigConstant.URL_PATTERN + getConnectionProperties(), polardbxAddress, polardbxPort);
        return JdbcUtil.createConnection(url, polardbxUser, polardbxPassword);
    }

    public Connection newPolarDBXConnectionWithUseAffectedRows() {
        String props = getConnectionProperties();
        if (props.isEmpty()) {
            props = "useAffectedRows=true";
        } else {
            props += "&useAffectedRows=true";
        }
        final String url =
            String.format(ConfigConstant.URL_PATTERN + props, polardbxAddress, polardbxPort);
        return JdbcUtil.createConnection(url, polardbxUser, polardbxPassword);
    }

    public Connection newPolarDBXConnectionWithExtraParams(String extraParams) {
        String url =
            String.format(ConfigConstant.URL_PATTERN + getConnectionProperties(), polardbxAddress, polardbxPort);
        url += extraParams;
        return JdbcUtil.createConnection(url, polardbxUser, polardbxPassword);
    }

    /**
     * get connection from a specific user
     */
    public Connection newPolarDBXConnection(String user, String password) {
        String url =
            String.format(ConfigConstant.URL_PATTERN + getConnectionProperties(), polardbxAddress, polardbxPort);
        return JdbcUtil.createConnection(url, user, password);
    }

    public Connection newMysqlConnection() {
        // Do not use MySQL cursor fetch mode to compare, since it has some bug in Date type handling. Using server
        // prepare mode instead.
        String connProp = getConnectionProperties();
        if (useCursorFetch()) {
            connProp = connProp.replace("useCursorFetch=true", "");
            connProp = connProp.replace("defaultFetchSize=1", "");
            connProp = connProp + "&useServerPrepStmts=true";
        }

        connProp = connProp.replace(POLARDBX_SERVER_ID_CONF, "");

        String url = String.format(ConfigConstant.URL_PATTERN + connProp, mysqlAddress, mysqlPort);
        return JdbcUtil.createConnection(url, mysqlUser, mysqlPassword);
    }

    public Connection newMysqlConnectionWithUseAffectedRows() {
        // Do not use MySQL cursor fetch mode to compare, since it has some bug in Date type handling. Using server
        // prepare mode instead.
        String connProp = getConnectionProperties();
        if (useCursorFetch()) {
            connProp = connProp.replace("useCursorFetch=true", "");
            connProp = connProp.replace("defaultFetchSize=1", "");
            connProp = connProp + "&useServerPrepStmts=true";
        }

        connProp = connProp.replace(POLARDBX_SERVER_ID_CONF, "");

        if (connProp.isEmpty()) {
            connProp = "useAffectedRows=true";
        } else {
            connProp += "&useAffectedRows=true";
        }
        String url = String.format(ConfigConstant.URL_PATTERN + connProp, mysqlAddress, mysqlPort);
        return JdbcUtil.createConnection(url, mysqlUser, mysqlPassword);
    }

    public Connection newMysqlConnectionWithExtraParams(String extraParams) {
        String connProp = getConnectionProperties();
        if (useCursorFetch()) {
            connProp = connProp.replace("useCursorFetch=true", "");
            connProp = connProp.replace("defaultFetchSize=1", "");
            connProp = connProp + "&useServerPrepStmts=true";
        }
        connProp = connProp.replace(POLARDBX_SERVER_ID_CONF, "");

        String url = String.format(ConfigConstant.URL_PATTERN + connProp, mysqlAddress, mysqlPort);
        url += extraParams;
        return JdbcUtil.createConnection(url, mysqlUser, mysqlPassword);
    }

    public Connection newMysqlConnectionSecond() {
        String connProp = getConnectionProperties();
        if (useCursorFetch()) {
            connProp = connProp.replace("useCursorFetch=true", "");
            connProp = connProp.replace("defaultFetchSize=1", "");
            connProp = connProp + "&useServerPrepStmts=true";
        }

        connProp = connProp.replace(POLARDBX_SERVER_ID_CONF, "");

        String url = String.format(ConfigConstant.URL_PATTERN + connProp, mysqlAddressSecond, mysqlPortSecond);
        return JdbcUtil.createConnection(url, mysqlUserSecond, mysqlPasswordSecond);

    }

    public void close() {
        this.mysqlDataSource.close();
        if (this.mysqlDataSourceSecond != null) {
            this.mysqlDataSourceSecond.close();
        }
        this.metaDataSource.close();
        this.polardbxDataSource.close();
    }

    @Override
    public String toString() {
        return "ConnectionManager{" +
            "inited=" + inited +
            ", mysqlUser='" + mysqlUser + '\'' +
            ", mysqlPassword='" + mysqlPassword + '\'' +
            ", mysqlPort='" + mysqlPort + '\'' +
            ", mysqlAddress='" + mysqlAddress + '\'' +
            ", mysqlUserSecond='" + mysqlUserSecond + '\'' +
            ", mysqlPasswordSecond='" + mysqlPasswordSecond + '\'' +
            ", mysqlPortSecond='" + mysqlPortSecond + '\'' +
            ", mysqlAddressSecond='" + mysqlAddressSecond + '\'' +
            ", polardbxUser='" + polardbxUser + '\'' +
            ", polardbxPassword='" + polardbxPassword + '\'' +
            ", polardbxPort='" + polardbxPort + '\'' +
            ", polardbxAddress='" + polardbxAddress + '\'' +
            ", metaUser='" + metaUser + '\'' +
            ", metaPassword='" + metaPassword + '\'' +
            ", metaPort='" + metaPort + '\'' +
            ", metaAddress='" + metaAddress + '\'' +
            '}';
    }

    public boolean checkSupportOpenSSL(Connection conn) {
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW STATUS LIKE 'Rsa_public_key'")) {
            return rs.next();
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
        return false;
    }

    public boolean isEnableOpenSSL() {
        return enableOpenSSL;
    }

    public String getPolardbxMode() {
        return polardbxMode;
    }

    public String getMysqlMode() {
        return mysqlMode;
    }

    public String getPolardbxUser() {
        return polardbxUser;
    }

    public String getPolardbxPort() {
        return polardbxPort;
    }

    public String getPolardbxAddress() {
        return polardbxAddress;
    }

    public String getMetaUser() {
        return metaUser;
    }

    public String getMetaPassword() {
        return metaPassword;
    }

    public String getMetaPort() {
        return metaPort;
    }

    public String getMetaAddress() {
        return metaAddress;
    }

    public String getMysqlPort() {
        return mysqlPort;
    }

    public String getMysqlAddress() {
        return mysqlAddress;
    }

    public String getMysqlPortSecond() {
        return mysqlPortSecond;
    }

    public String getMysqlAddressSecond() {
        return mysqlAddressSecond;
    }
}
