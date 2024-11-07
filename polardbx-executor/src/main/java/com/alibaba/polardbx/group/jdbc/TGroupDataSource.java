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

package com.alibaba.polardbx.group.jdbc;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.DBType;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.Version;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.group.config.OptimizedGroupConfigManager;
import com.alibaba.polardbx.group.config.Weight;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.google.common.annotations.VisibleForTesting;

import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * @author 梦实 2017年12月12日 下午2:01:21
 * @since 5.0.0
 */
public class TGroupDataSource extends AbstractLifecycle implements IDataSource, Lifecycle {
    public static final String LOCAL_ADDRESS = "127.0.0.1:3306";

    private static final Logger logger = LoggerFactory.getLogger(TGroupDataSource.class);

    public static final String INVALID_ADDRESS = "127.0.0.1:3306";

    public static final String VERSION = "2.4.1";

    public static final String PREFIX = "com.taobao.tddl.jdbc.group_V" + VERSION + "_";

    private OptimizedGroupConfigManager configManager;

    public boolean mock = false;

    /**
     * 下面三个为一组，支持本地配置
     */
    private String dsKeyAndWeightCommaArray;
    private DataSourceFetcher dataSourceFetcher;
    private DBType dbType = DBType.MYSQL;
    private String schemaName;
    private String appName;                                                              // app名字
    private String unitName;  // 单元化名字
    private String dbGroupKey;
    private String fullDbGroupKey = null;                                          // dataId

    private boolean stressTestValid = false;

    // 下面两个字段当建立实际的DataSource时必须传递过去
    // jdbc规范: DataSource刚建立时LogWriter为null
    private PrintWriter out = null;
    // jdbc规范: DataSource刚建立时LoginTimeout为0
    private int seconds = 0;

    private String url = null;

    @Deprecated
    public TGroupDataSource() {
    }

    public TGroupDataSource(String dbGroupKey, String schemaName, String appName, String unitName) {
        this.dbGroupKey = dbGroupKey;
        this.appName = appName;
        this.schemaName = schemaName;
        if (unitName != null) {
            unitName = unitName.trim();
        }
        this.unitName = unitName;
    }

    /**
     * 基于dbGroupKey、appName来初始化多个TAtomDataSource
     */
    @Override
    public void doInit() {
        Version.checkVersion();
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("--------------");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("TGroupDataSource start init");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("appName is: " + appName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("unitName is: " + unitName);
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("dbGroupKey is: " + this.dbGroupKey);

        if (url != null && !"".equalsIgnoreCase(url)) {
            parseUrl(url);
        }
        checkProperties();
        configManager = new OptimizedGroupConfigManager(this);
        configManager.init();
    }

    @VisibleForTesting
    protected void setConfigManager(OptimizedGroupConfigManager configManager) {
        this.configManager = configManager;
    }

    /**
     * 如果构造的是TAtomDataSource，必须检查dbGroupKey、appName两个属性的值是否合法
     */
    private void checkProperties() {
        if (dbGroupKey == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SET_GROUPKEY);
        }
        dbGroupKey = dbGroupKey.trim();
        if (dbGroupKey.length() < 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SET_GROUPKEY);
        }

        if (appName == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SET_APPNAME);
        }
        appName = appName.trim();
        if (appName.length() < 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SET_APPNAME);
        }

    }

    @Override
    public TGroupDirectConnection getConnection() throws SQLException {
        return getConnection(
            ConfigDataMode.isMasterMode() ? MasterSlave.MASTER_ONLY : MasterSlave.SLAVE_ONLY);
    }

    @Override
    public TGroupDirectConnection getConnection(MasterSlave masterSlave) throws SQLException {

        if (!isInited() && url != null) {// 没有经过正常参数启动且URL不为空时，则尝试使用URL启动
            parseUrl(url);
            init();
        }

        TGroupDirectConnection connection = new TGroupDirectConnection(this, masterSlave);
        connection.setStressTestValid(stressTestValid);
        return connection;
    }

    private void parseUrl(String url) {
        if (!url.startsWith("jdbc:tddl:tgroupdatasource")) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "不支持的URL，请使用jdbc:tddl:tgroupdatasource:前缀");
        }

        int beginningOfSlashes = url.indexOf("//");

        int index = url.indexOf("?");
        String hostStuff;
        String configNames;
        String propsIter;
        Properties urlProps = new Properties();
        if (index != -1) {
            hostStuff = url.substring(index + 1);
            url = url.substring(0, index);
            StringTokenizer slashIndex = new StringTokenizer(hostStuff, "&");

            while (slashIndex.hasMoreTokens()) {
                String numHosts = slashIndex.nextToken();
                int propertiesTransformClassName = com.mysql.jdbc.StringUtils.indexOfIgnoreCase(0, numHosts, "=");
                configNames = null;
                propsIter = null;
                if (propertiesTransformClassName != -1) {
                    configNames = numHosts.substring(0, propertiesTransformClassName);
                    if (propertiesTransformClassName + 1 < numHosts.length()) {
                        propsIter = numHosts.substring(propertiesTransformClassName + 1);
                    }
                }

                if (propsIter != null && propsIter.length() > 0 && configNames != null && configNames.length() > 0) {

                    try {
                        urlProps.put(configNames, URLDecoder.decode(propsIter, "UTF-8"));
                    } catch (UnsupportedEncodingException var21) {
                        urlProps.put(configNames, URLDecoder.decode(propsIter));
                    } catch (NoSuchMethodError var22) {
                        urlProps.put(configNames, URLDecoder.decode(propsIter));
                    }
                }
            }
        }

        String nameStr = url.substring(beginningOfSlashes + 2);
        int slashIndex = nameStr.indexOf("/");
        appName = nameStr.substring(0, slashIndex);
        dbGroupKey = nameStr.substring(slashIndex + 1);

        LoggerInit.TDDL_DYNAMIC_CONFIG.info(
            "parse tgroupdatasource by url:" + this.url + ", get appname:" + appName + ", dbGroupKey:" + dbGroupKey);
    }

    @Override
    public IConnection getConnection(String username, String password) throws SQLException {
        return getConnection(username, password,
            ConfigDataMode.isMasterMode() ? MasterSlave.MASTER_ONLY : MasterSlave.SLAVE_ONLY);
    }

    public IConnection getConnection(String username, String password, MasterSlave master) throws SQLException {

        if (!isInited() && url != null) {// 没有经过正常参数启动且URL不为空时，则尝试使用URL启动
            parseUrl(url);
            init();
        }

        TGroupDirectConnection connection = new TGroupDirectConnection(this, master, username, password);
        connection.setStressTestValid(stressTestValid);
        return connection;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return out;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.out = out;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return seconds;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.seconds = seconds;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {

        if (unitName != null) {
            unitName = unitName.trim();
        }
        this.unitName = unitName;
    }

    public String getAppName() {

        return appName;
    }

    public void setAppName(String appName) {
        if (appName != null) {
            appName = appName.trim();
        }
        this.appName = appName;
    }

    public String getDbGroupKey() {
        return dbGroupKey;
    }

    public String getFullDbGroupKey() {
        if (fullDbGroupKey == null) {
            fullDbGroupKey = PREFIX + getDbGroupKey();
        }

        return fullDbGroupKey;
    }

    public void setDbGroupKey(String dbGroupKey) {
        if (dbGroupKey != null) {
            dbGroupKey = dbGroupKey.trim();
        }

        this.dbGroupKey = dbGroupKey;
    }

    public void setDataSourceFetcher(DataSourceFetcher dataSourceFetcher) {
        this.dataSourceFetcher = dataSourceFetcher;
    }

    public void setDbType(DBType dbType) {
        this.dbType = dbType;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public DBType getDbType() {
        return dbType;
    }

    /**
     * 销毁数据源，慎用
     */
    public void destroyDataSource() {

        /**
         * Unbound the config listeners of group
         */
        if (this.configManager != null) {
            configManager.unbindGroupConfigListener();
        }

        /**
         * destroy the datasources of group
         */
        destroy();
    }



    @Override
    protected void doDestroy() {
        if (configManager != null) {
            configManager.destroyDataSource();
        }
    }

    public boolean isStressTestValid() {
        return stressTestValid;
    }

    public void setStressTestValid(boolean stressTestValid) {
        this.stressTestValid = stressTestValid;
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new UnsupportedOperationException("getParentLogger");
    }

    public Map<TAtomDataSource, Weight> getAtomDataSourceWeights() {
        Map<TAtomDataSource, Weight> atomDataSources = new HashMap<TAtomDataSource, Weight>();

        for (DataSourceWrapper wrapper : this.configManager.getDataSourceWrapperMap().values()) {
            atomDataSources.put(wrapper.getWrappedDataSource(), wrapper.getWeight());
        }
        return atomDataSources;
    }

    public List<TAtomDataSource> getAtomDataSources() {
        List<TAtomDataSource> atomDataSources = new ArrayList<TAtomDataSource>(
            this.configManager.getDataSourceWrapperMap().size());

        for (DataSourceWrapper wrapper : this.configManager.getDataSourceWrapperMap().values()) {
            atomDataSources.add(wrapper.getWrappedDataSource());
        }
        return atomDataSources;
    }

    public TAtomDataSource getOneAtomDs(boolean master) {
        TAtomDataSource ds = null;
        if (master) {
            for (DataSourceWrapper wrapper : this.configManager.getDataSourceWrapperMap().values()) {
                if (wrapper.getWeight() != null && wrapper.getWeight().w > 0) {
                    ds = wrapper.getWrappedDataSource();
                    break;
                }
            }
        } else {
            for (DataSourceWrapper wrapper : this.configManager.getDataSourceWrapperMap().values()) {
                if (wrapper.getWeight() != null && (wrapper.getWeight().w <= 0 && wrapper.getWeight().r > 0)) {
                    ds = wrapper.getWrappedDataSource();
                    break;
                }
            }
        }

        if (ds == null) {
            //The write weight is always > 0 for MetaDB Connection.
            logger.warn(
                "Not as expected for the appname:" + appName + ", dbGroupKey:" + dbGroupKey);
            for (DataSourceWrapper wrapper : this.configManager.getDataSourceWrapperMap().values()) {
                ds = wrapper.getWrappedDataSource();
                if (ds != null) {
                    break;
                }
            }
        }

        return ds;
    }

    public String getOneAtomAddress(boolean master) {
        TAtomDataSource ds = null;
        if (master) {
            for (DataSourceWrapper wrapper : this.configManager.getDataSourceWrapperMap().values()) {
                if (wrapper.getWeight() != null && wrapper.getWeight().w > 0) {
                    ds = wrapper.getWrappedDataSource();
                    break;
                }
            }
        } else {
            for (DataSourceWrapper wrapper : this.configManager.getDataSourceWrapperMap().values()) {
                if (wrapper.getWeight() != null && (wrapper.getWeight().w <= 0 && wrapper.getWeight().r > 0)) {
                    ds = wrapper.getWrappedDataSource();
                    break;
                }
            }
        }

        if (ds == null) {
            //The write weight is always > 0 for MetaDB Connection.
            logger.warn(
                "Not as expected for the appname:" + appName + ", dbGroupKey:" + dbGroupKey);
            for (DataSourceWrapper wrapper : this.configManager.getDataSourceWrapperMap().values()) {
                ds = wrapper.getWrappedDataSource();
                if (ds != null) {
                    break;
                }
            }
        }

        if (ds != null) {
            return ds.getHost() + ':' + ds.getPort();
        } else {
            // 不可能一个分库没有任何一个数据源，不应该走到这个逻辑
            return INVALID_ADDRESS;
        }
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public OptimizedGroupConfigManager getConfigManager() {
        return configManager;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public boolean isXDataSource() {
        if (this.configManager.getDataSourceWrapperMap().isEmpty()) {
            return false;
        }
        for (DataSourceWrapper wrapper : this.configManager.getDataSourceWrapperMap().values()) {
            if (!(wrapper.getWrappedDataSource().getDataSource() instanceof XDataSource)) {
                throw new AssertionError("unreachable");
            }
        }
        return true;
    }

    /**
     * Get instance id of underlying write node
     *
     * @return instance id (HOST:PORT)
     */
    public String getMasterSourceAddress() {

        String instanceId = null;
        for (DataSourceWrapper wrapper : this.configManager.getDataSourceWrapperMap().values()) {
            Weight w = wrapper.getWeight();
            if (w != null && w.w != 0) {
                instanceId = wrapper.getWrappedDataSource().getHost() + ":" + wrapper.getWrappedDataSource().getPort();
                break;
            }
        }
        return instanceId;
    }

    /**
     * Don't cache the masterDNID, because the {@link TGroupDataSource} don't rebuild in scale-out.
     */
    @Override
    public String getMasterDNId() {
        String masterDNId = null;
        for (DataSourceWrapper wrapper : this.configManager.getDataSourceWrapperMap().values()) {
            Weight w = wrapper.getWeight();
            if (w != null && w.w != 0) {
                masterDNId = wrapper.getWrappedDataSource().getDnId();
                break;
            }
        }
        return masterDNId;
    }

    @Override
    public int hashCode() {
        // schema, group name, host:port
        int result = 1;
        String masterAddr = getMasterSourceAddress();
        result = 31 * result + (schemaName == null ? 0 : schemaName.hashCode());
        result = 31 * result + (dbGroupKey == null ? 0 : dbGroupKey.hashCode());
        result = 31 * result + (masterAddr == null ? 0 : masterAddr.hashCode());
        return result;
    }
}
