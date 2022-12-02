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

package com.alibaba.polardbx.gms.metadb;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.HaSwitcher;
import com.alibaba.polardbx.gms.ha.impl.StorageHaChecker;
import com.alibaba.polardbx.gms.ha.impl.StorageNodeHaInfo;
import com.alibaba.polardbx.gms.ha.impl.StorageRole;
import com.alibaba.polardbx.gms.topology.InstConfigAccessor;
import com.alibaba.polardbx.gms.topology.InstConfigRecord;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.GmsJdbcUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.client.XClient;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author chenghui.lch
 */
public class MetaDbDataSource extends AbstractLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(MetaDbDataSource.class);

    public static final String DEFAULT_META_DB_GROUP_NAME = "__META_DB__";
    public final static String DEFAULT_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    public final static String DEFAULT_VALIDATION_QUERY = "select 'x'";
    public static final String DEFAULT_META_DB_NAME = "polardbx_meta_db";
    public static final String DEFAULT_META_DB_PROPS =
        GmsJdbcUtil.getJdbcConnPropsFromPropertiesMap(GmsJdbcUtil.getDefaultConnPropertiesForMetaDb());
    protected static MetaDbDataSource instance;

    protected List<String> metaDbAddrList;
    protected String metaDbName;
    protected String metaDbProp;
    protected String metaDbUser;
    protected String metaDbEncPasswd;

    // HA Wrapper for the physical DataSource of MetaDb
    protected MetaDbDataSourceHaWrapper physicalDataSourceHaWrapper;
    // Use to dynamic config the druid of MetaDb
    protected MetaDbConnConf conf;

    protected volatile int metaDbStorageType = -1;
    protected volatile String metaDbVipAddrStr;

    protected volatile Pair<String, Integer> metaDbAvailableAddr;
    protected volatile int metaDbXport = -1;
    protected MetaDbHaSwitcher metaDbHaSwitcher = new MetaDbHaSwitcher(this);

    protected MetaDbDataSource(String addrListStr, String dbName, String properties, String user, String passwd) {
        this.metaDbAddrList = getMetaDbAddrInfo(addrListStr);
        this.metaDbName = dbName;
        this.metaDbProp = properties;
        this.metaDbUser = user;
        this.metaDbEncPasswd = passwd;
        this.conf = new MetaDbConnConf();
    }

    protected List<String> getMetaDbAddrInfo(String addrListStr) {
        List<String> addrList = new ArrayList<>();
        String[] addrArr = addrListStr.split(",");
        for (int i = 0; i < addrArr.length; i++) {
            String addrArrTrim = addrArr[i].trim();
            if (addrArrTrim.isEmpty()) {
                continue;
            }
            addrList.add(addrArrTrim);
        }
        return addrList;
    }

    @Override
    protected void doInit() {
        logger.info("initializing metaDb dataSource");
        initMetaDbAvaliableAddr();
        this.physicalDataSourceHaWrapper =
            new MetaDbDataSourceHaWrapper(initMetaDbDataSource(this.metaDbAvailableAddr, this.metaDbXport));
    }

    private void initTsoServicesX(XDataSource dataSource) {
        try (Connection conn = dataSource.getConnection()) {
            if (conn.unwrap(XConnection.class).getSession().getClient().getBaseVersion()
                == XClient.DnBaseVersion.DN_RDS_80_X_CLUSTER || XConfig.GALAXY_X_PROTOCOL || XConfig.OPEN_XRPC_PROTOCOL) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate("create sequence mysql.gts_base cache 2 TIMESTAMP");
                }
            }
        } catch (SQLException exception) {
            if (!exception.getMessage().contains("already exists")) {
                logger.error(exception);
                throw new TddlNestableRuntimeException(exception);
            }
        }
    }

    private void initTsoServicesJDBC(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection()) {
            try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select version()")) {
                if (rs.next() && rs.getString(1).startsWith("8.0")) {
                    rs.close();
                    stmt.executeUpdate("create sequence mysql.gts_base cache 2 TIMESTAMP");
                }
            }
        } catch (SQLException exception) {
            if (!exception.getMessage().contains("already exists")) {
                logger.error(exception);
                throw new TddlNestableRuntimeException(exception);
            }
        }
    }

    private void initXDataSourceByJdbcProps(XDataSource dataSource, String prop, MetaDbConnConf metaDbConnConf) {
        final Map<String, String> map = GmsJdbcUtil.getPropertiesMapFromJdbcConnProps(prop);
        final String encoding = map.get("characterEncoding");
        if (encoding != null) {
            dataSource.setDefaultEncodingMySQL(encoding);
        } else {
            dataSource.setDefaultEncodingMySQL("utf8mb4");
        }
        final String connTimeout = map.get("connectTimeout");
        if (connTimeout != null) {
            try {
                dataSource.setGetConnTimeoutMillis(Long.parseLong(connTimeout));
            } catch (Exception e) {
                logger.error(e);
            }
        } else if (metaDbConnConf.blockingTimeout > 0) {
            dataSource.setGetConnTimeoutMillis(metaDbConnConf.blockingTimeout);
        }
        final String queryTimeout = map.get("socketTimeout");
        if (queryTimeout != null) {
            try {
                dataSource.setDefaultQueryTimeoutMillis(Integer.parseInt(queryTimeout));
            } catch (Exception e) {
                logger.error(e);
            }
        } else {
            dataSource.setDefaultQueryTimeoutNanos(XConfig.DEFAULT_TIMEOUT_NANOS);
        }

        // Init TSO service.
        initTsoServicesX(dataSource);
    }

    protected DataSource initMetaDbDataSource(Pair<String, Integer> metaDbAvailableAddr, int xport) {
        // Note: Xproto is enabled when property in server.properties is enabled.
        final int defaultXport = XConnectionManager.getInstance().getMetaDbPort(); // Global switch.
        if (defaultXport > 0 || (0 == defaultXport && xport > 0)) {
            final XDataSource newDs =
                new XDataSource(metaDbAvailableAddr.getKey(), defaultXport > 0 ? defaultXport : xport,
                    this.metaDbUser, PasswdUtil.decrypt(this.metaDbEncPasswd),
                    this.metaDbName, "metaDbXDataSource");
            initXDataSourceByJdbcProps(newDs, this.metaDbProp, this.conf);
            return newDs;
        } else {
            throw new NotSupportException("not supported");
        }
    }

    protected DataSource buildAndInitMetaDbDataSource(Pair<String, Integer> metaDbAvailableAddr, int xport) {
        // Note: Xproto is enabled when property in server.properties is enabled.
        final int defaultXport = XConnectionManager.getInstance().getMetaDbPort(); // Global switch.
        if (defaultXport > 0 || (0 == defaultXport && xport > 0)) {
            final XDataSource newDs =
                new XDataSource(metaDbAvailableAddr.getKey(), defaultXport > 0 ? defaultXport : xport,
                    this.metaDbUser, PasswdUtil.decrypt(this.metaDbEncPasswd),
                    this.metaDbName, "metaDbXDataSource");
            initXDataSourceByJdbcProps(newDs, this.metaDbProp, this.conf);
            return newDs;
        } else {
            throw new NotSupportException("jdbc not supported");
        }
    }

    protected class MetaDbHaSwitcher implements HaSwitcher {

        protected MetaDbDataSource metaDbDataSource = null;

        public MetaDbHaSwitcher(MetaDbDataSource metaDbDataSource) {
            this.metaDbDataSource = metaDbDataSource;
        }

        @Override
        public void doHaSwitch(HaSwitchParams haSwitchParams) {
            String availableAddr = haSwitchParams.curAvailableAddr;

            // check if current available addr is the same as new available addr
            String curLeaderAddrStr =
                AddressUtils.getAddrStrByIpPort(this.metaDbDataSource.metaDbAvailableAddr.getKey(),
                    this.metaDbDataSource.metaDbAvailableAddr.getValue());
            if (availableAddr.equalsIgnoreCase(curLeaderAddrStr)
                && haSwitchParams.xport == this.metaDbDataSource.metaDbXport) {
                return;
            }

            MetaDbLogUtil.META_DB_LOG.info("MetaDB HA cur:" + curLeaderAddrStr + " to:" + availableAddr + " old xport:"
                + this.metaDbDataSource.metaDbXport + " new xport:" + haSwitchParams.xport);

            // build new datasource by new available addr
            Pair<String, Integer> newAvailableIpPort = AddressUtils.getIpPortPairByAddrStr(availableAddr);
            DataSource newDataSource = buildAndInitMetaDbDataSource(newAvailableIpPort, haSwitchParams.xport);

            // use new datasource to refresh old datasource
            MetaDbDataSourceHaWrapper haWrapper = this.metaDbDataSource.physicalDataSourceHaWrapper;
            DataSource oldDruidDataSource = haWrapper.getRawDataSource();
            haWrapper.refreshDataSource(newDataSource);

            // update current available addr
            this.metaDbDataSource.metaDbAvailableAddr = newAvailableIpPort;
            this.metaDbDataSource.metaDbXport = haSwitchParams.xport;

            ((XDataSource) oldDruidDataSource).close();
        }
    }

    protected void initMetaDbAvaliableAddr() {

        List<String> addrListOfMetaStorage = new ArrayList<>();
        List<StorageInfoRecord> storageInfoRecordsWithVip;
        List<StorageInfoRecord> storageInfoRecords = new ArrayList<>();
        if (this.metaDbAddrList.size() == 1) {
            // leaderAddr maybe a vip
            String leaderAddr = metaDbAddrList.get(0);
            Pair<String, Integer> ipPort = AddressUtils.getIpPortPairByAddrStr(leaderAddr);

            // Fetch all storage host list from metadb by the metaDbAddrList set by startup.sh params
            try (Connection metaDbConn = GmsJdbcUtil
                .buildJdbcConnection(ipPort.getKey(), ipPort.getValue(), this.metaDbName, this.metaDbUser,
                    this.metaDbEncPasswd, MetaDbDataSource.DEFAULT_META_DB_PROPS)) {
                // Get metaDB x config if server.properties set to auto.
                if (0 == XConnectionManager.getInstance().getMetaDbPort()) {
                    try {
                        final InstConfigAccessor instConfigAccessor = new InstConfigAccessor();
                        instConfigAccessor.setConnection(metaDbConn);
                        final String instId = InstIdUtil.getInstId();
                        final List<InstConfigRecord> allInstConfigsByInstId =
                            instConfigAccessor.getAllInstConfigsByInstId(instId);
                        allInstConfigsByInstId.stream()
                            .filter(item -> item.paramKey
                                .equalsIgnoreCase(ConnectionProperties.CONN_POOL_XPROTO_META_DB_PORT))
                            .findAny()
                            .ifPresent(item -> XConnectionManager.getInstance()
                                .setMetaDbPort(Integer.parseInt(item.paramVal)));
                    } catch (Exception e) {
                        logger.warn("Fail to load metaDB Xport from inst_config, caused by " + e.getMessage());
                        MetaDbLogUtil.META_DB_LOG.warn(e);
                    }
                }
                MetaDbLogUtil.META_DB_LOG
                    .info("XConnectionManager init metaDB port: " + XConnectionManager.getInstance().getMetaDbPort());

                try {
                    StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
                    storageInfoAccessor.setConnection(metaDbConn);
                    storageInfoRecordsWithVip =
                        storageInfoAccessor.getStorageInfosByInstKind(StorageInfoRecord.INST_KIND_META_DB);
                } catch (Exception e) {
                    // When initialize gms, this table will not exist
                    if (e.getMessage().contains("doesn't exist")) {
                        this.metaDbAvailableAddr = ipPort;
                        return;
                    } else {
                        throw e;
                    }
                }

                for (int i = 0; i < storageInfoRecordsWithVip.size(); i++) {
                    StorageInfoRecord storageInfo = storageInfoRecordsWithVip.get(i);
                    if (this.metaDbStorageType == -1) {
                        this.metaDbStorageType = storageInfo.storageType;
                    }
                    if (storageInfo.isVip == StorageInfoRecord.IS_VIP_TRUE) {
                        this.metaDbVipAddrStr = AddressUtils.getAddrStrByIpPort(storageInfo.ip, storageInfo.port);
                        if (storageInfo.storageType == StorageInfoRecord.STORAGE_TYPE_XCLUSTER ||
                            storageInfo.storageType == StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER ||
                            storageInfo.storageType == StorageInfoRecord.STORAGE_TYPE_GALAXY_CLUSTER) {
                            // if current storage inst is a xcluster inst,
                            // then its vip info should be ignored in getStorageRole info
                            continue;
                        }
                    }
                    storageInfoRecords.add(storageInfo);
                }

                int storageInfoSize = storageInfoRecords.size();
                if (storageInfoSize == 0 || storageInfoSize == 1) {
                    /**
                     * The storage of meta db has only one a vip addr,
                     * and the storage type is not a x-cluster,
                     * just use the vip as metaDbAvailableAddr
                     */
                    if (this.metaDbStorageType != StorageInfoRecord.STORAGE_TYPE_XCLUSTER &&
                        this.metaDbStorageType != StorageInfoRecord.STORAGE_TYPE_RDS80_XCLUSTER &&
                        this.metaDbStorageType != StorageInfoRecord.STORAGE_TYPE_GALAXY_CLUSTER) {
                        if (this.metaDbVipAddrStr == null) {
                            // No find any vip, use the only one storage info
                            if (storageInfoSize == 1) {
                                StorageInfoRecord storageInfo = storageInfoRecords.get(0);
                                this.metaDbVipAddrStr =
                                    AddressUtils.getAddrStrByIpPort(storageInfo.ip, storageInfo.port);
                            }
                        }

                        if (this.metaDbVipAddrStr != null) {
                            metaDbAvailableAddr = AddressUtils.getIpPortPairByAddrStr(this.metaDbVipAddrStr);
                            if (XConfig.VIP_WITH_X_PROTOCOL) {
                                metaDbXport = metaDbAvailableAddr.getValue();
                            } else if ((XConfig.GALAXY_X_PROTOCOL || XConfig.OPEN_XRPC_PROTOCOL) &&
                                StorageInfoRecord.STORAGE_TYPE_GALAXY_SINGLE == this.metaDbStorageType) {
                                // Read from storage info if single galaxy engine.
                                final StorageInfoRecord storageInfo = storageInfoRecords.get(0);
                                metaDbXport = storageInfo.xport;
                            } else {
                                metaDbXport = -1;
                            }
                            MetaDbLogUtil.META_DB_LOG.info(
                                "Single node with type: " + this.metaDbStorageType + " addr: " + this.metaDbVipAddrStr
                                    + (XConfig.GALAXY_X_PROTOCOL ? " with galaxy" : "") + (XConfig.OPEN_XRPC_PROTOCOL ?
                                    " with xrpc" : "") + " xport: " + metaDbXport);
                        }
                        return;
                    }
                }

            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
            }
        } else {
            addrListOfMetaStorage = this.metaDbAddrList;
        }

        /**
         * Remove duplicate addr of metaDb storage
         */
        Set<String> addrSetOfMetaDbStorage = new HashSet<>();
        addrSetOfMetaDbStorage.addAll(addrListOfMetaStorage);
        for (int i = 0; i < storageInfoRecords.size(); i++) {
            StorageInfoRecord storageInfoRecord = storageInfoRecords.get(i);
            String addrOfStorage = AddressUtils.getAddrStrByIpPort(storageInfoRecord.ip, storageInfoRecord.port);
            if (!addrSetOfMetaDbStorage.contains(addrOfStorage)) {
                addrListOfMetaStorage.add(addrOfStorage);
                addrSetOfMetaDbStorage.add(addrOfStorage);
            }
        }

        /**
         * Try to fetch leader info by all addr infos and vip addr of metaDb
         * More than one addresses when get here so it can't be single node(Set xport to -1).
         */
        Map<String, StorageNodeHaInfo>
            addrRoleInfoMap = StorageHaChecker
            .checkAndFetchRole(addrListOfMetaStorage, this.metaDbVipAddrStr, -1, this.metaDbUser,
                PasswdUtil.decrypt(this.metaDbEncPasswd),
                this.metaDbStorageType, StorageInfoRecord.INST_KIND_META_DB, true);

        /**
         * Find leader info from addrRoleInfo fetched by StorageHaChecker.checkAndFetchRole
         */
        String leaderAddrStr = null;
        int dynamicXPort = -1;
        for (Map.Entry<String, StorageNodeHaInfo> addrRoleInfo : addrRoleInfoMap.entrySet()) {
            String addr = addrRoleInfo.getKey();
            StorageNodeHaInfo haInfo = addrRoleInfo.getValue();
            if (haInfo.getRole() == StorageRole.LEADER) {
                leaderAddrStr = addr;
                dynamicXPort = haInfo.getXPort();
                break;
            }
        }

        if (leaderAddrStr == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("No found any leader for metadb storage: addr=%s, db=%s, user=%s, passwd=%s",
                    this.metaDbAddrList, this.metaDbName, this.metaDbUser, this.metaDbEncPasswd));
        }
        Pair<String, Integer> leaderAddr = AddressUtils.getIpPortPairByAddrStr(leaderAddrStr);

        this.metaDbAvailableAddr = leaderAddr;
        if (leaderAddrStr.equals(this.metaDbVipAddrStr)) {
            if (XConfig.VIP_WITH_X_PROTOCOL) {
                this.metaDbXport = this.metaDbAvailableAddr.getValue();
            } else if (XConfig.GALAXY_X_PROTOCOL || XConfig.OPEN_XRPC_PROTOCOL) {
                this.metaDbXport = dynamicXPort;
            } else {
                this.metaDbXport = -1;
            }
        } else {
            this.metaDbXport = dynamicXPort;
        }
    }

    public static void initSystemDbIfNeed() {
        logger.info("checking defaultDb");
        SystemDbHelper.checkOrCreateDefaultDb(MetaDbDataSource.getInstance());

        logger.info("checking infoSchemaDb");
        SystemDbHelper.checkOrCreateInfoSchemaDb(MetaDbDataSource.getInstance());

        String CDC_STARTUP_MODE =
            MetaDbInstConfigManager.getInstance().getInstProperty(ConnectionProperties.CDC_STARTUP_MODE);
        // CDC_STARTUP_MODE为1, 同步启动CDC
        if (Integer.valueOf(CDC_STARTUP_MODE) == 1) {
            logger.info("Start cdc synchronously: checking cdcDb");
            SystemDbHelper.checkOrCreateCdcDb(MetaDbDataSource.getInstance());
        }
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
        if (physicalDataSourceHaWrapper != null) {
            if (physicalDataSourceHaWrapper.getRawDataSource() != null) {
                DataSource ds = physicalDataSourceHaWrapper.getRawDataSource();
                ((XDataSource) ds).close();
            }
        }
    }

    public Connection getConnection() {
        try {
            Connection conn = physicalDataSourceHaWrapper.getConnection();
            return conn;
        } catch (Throwable ex) {
            // Should log to a special log file for meta db
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, ex, ex.getMessage());
        }
    }

    public DataSource getDataSource() {
        return physicalDataSourceHaWrapper;
    }

    public static MetaDbDataSource getInstance() {
        if (instance == null) {
            return null;
        }
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    public static void initMetaDbDataSource(String addrs, String dbName, String props, String usr, String pwd) {

        if (addrs == null || addrs.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "Init meta db error, url is null");
        }

        if (usr == null || usr.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "Init meta db error, user is null");
        }

        if (pwd == null || pwd.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "Init meta db error, passwd is null");
        }

        MetaDbDataSource metaDbDataSource = new MetaDbDataSource(addrs, dbName, props, usr, pwd);
        instance = metaDbDataSource;

        // Init meta db datasource
        try {
            MetaDbDataSource.getInstance();
            MetaDbLogUtil.META_DB_LOG.info(String.format("Init metadb: addrs=%s dbName=%s user=%s passwd=%s",
                addrs, dbName, usr, pwd));
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error("Failed to init meta db, err is", ex);
        }

        // refresh metaDbConn by new Conn pool config

    }

    /**
     * A HA Wrapper for real datasource of metaDB
     * <p>
     * <pre>
     * The reference of metaDb datasource will be used by all kind of managers
     * (such as GsiManager/StatisticsManager/TableMetaManager...., and so on ).
     * If the datasource need doing master-slave switch for HA, all these managers
     * that have referenced the metaDb datasource need to update its reference of datasource.
     * This will be a complicated operation.
     * To avoiding this situation, use  MetaDbDataSourceHaWrapper that has implement the interface
     * of DataSource to wrapper the real druid datasource and handle the HA inside.
     * So the HA of metaDb datasource will be transparent to those managers.
     *
     * </pre>
     */
    public static class MetaDbDataSourceHaWrapper implements DataSource {

        protected volatile DataSource physicalDataSource; // May Druid or XDataSource.

        public MetaDbDataSourceHaWrapper(DataSource druidDs) {
            this.physicalDataSource = druidDs;
        }

        public void refreshDataSource(DataSource newDruidDs) {
            this.physicalDataSource = newDruidDs;
        }

        public DataSource getRawDataSource() {
            return physicalDataSource;
        }

        @Override
        public Connection getConnection() throws SQLException {
            return physicalDataSource.getConnection();
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return physicalDataSource.getConnection(username, password);
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return physicalDataSource.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return physicalDataSource.isWrapperFor(iface);
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return physicalDataSource.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            physicalDataSource.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            physicalDataSource.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return physicalDataSource.getLoginTimeout();
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return physicalDataSource.getParentLogger();
        }
    }

    public HaSwitcher getMetaDbHaSwitcher() {
        return metaDbHaSwitcher;
    }

    public MetaDbConnConf getConf() {
        return this.conf;
    }

    public String getMetaDbName() {
        return metaDbName;
    }

}
