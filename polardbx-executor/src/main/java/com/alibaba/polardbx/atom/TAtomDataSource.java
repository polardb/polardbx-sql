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

package com.alibaba.polardbx.atom;

import com.alibaba.polardbx.atom.common.TAtomConstants;
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.atom.config.TAtomDsConfHandle;
import com.alibaba.polardbx.atom.config.listener.AtomAppConfigChangeListener;
import com.alibaba.polardbx.atom.config.listener.AtomDbStatusListener;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.rpc.pool.XConnection;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 梦实 2018年1月9日 下午3:46:12
 * @since 5.0.0
 */
public class TAtomDataSource extends AbstractTAtomDataSource {

    protected static Logger logger = LoggerFactory.getLogger(TAtomDataSource.class);
    private static final Map<String, TAtomDsConfHandle> cacheConfHandleMap =
        new ConcurrentHashMap<String, TAtomDsConfHandle>();
    private volatile TAtomDsConfHandle dsConfHandle = null;

    // 记录一下属于哪个groupKey
    private String groupKey;

    // 用于标记是否将所有下层连接池关闭, 主要为了双引擎而做
    private static final boolean off = false;

    private AtomAppConfigChangeListener appConfigChangeListener;

    private String dsModeInGroupKey = null;

    //这个变量是给集团mpp 集群使用，使用的时候务必小心
    @Deprecated
    private boolean masterDB = true;

    private String dnId;

    public TAtomDataSource() {
        dsConfHandle = new TAtomDsConfHandle(this);
    }

    public TAtomDataSource(boolean masterDB) {
        this.masterDB = masterDB;
        dsConfHandle = new TAtomDsConfHandle(this);
    }

    @Override
    public void init(String appName, String dsKey, String unitName) {
        setAppName(appName);
        setDbKey(dsKey);
        setUnitName(unitName);
        init();
    }

    @Override
    public void init(String appName, String groupKey, String dsKey, String unitName) {
        setAppName(appName);
        setDbKey(dsKey);
        setUnitName(unitName);
        this.groupKey = groupKey;
        init();
    }

    public void init(String appName, String groupKey, String dsKey, String unitName, TAtomDsConfDO atomDsConf) {
        this.dsConfHandle = new TAtomDsConfHandle(this, atomDsConf);
        setAppName(appName);
        setDbKey(dsKey);
        setUnitName(unitName);
        this.groupKey = groupKey;

        init();
    }

    @Override
    public void doInit() {

        LoggerInit.TDDL_DYNAMIC_CONFIG.info("--------------");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("TAtomDataSource start init");
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("appName is: " + this.getAppName());
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("unitName is: " + this.getUnitName());
        LoggerInit.TDDL_DYNAMIC_CONFIG.info("dbGroupKey is: " + this.getDbKey());

        String dbName = TAtomConstants.getDbNameStr(this.getUnitName(), this.getAppName(), this.getDbKey());

        synchronized (cacheConfHandleMap) {
            TAtomDsConfHandle cacheConfHandle = cacheConfHandleMap.get(dbName);
            if (null == cacheConfHandle) {

                // 初始化config的管理器
                this.dsConfHandle.init();
                cacheConfHandleMap.put(dbName, dsConfHandle);
                if (off) {
                    // 如果已经关闭了所有的连接, 则新建的也关闭掉
                    dsConfHandle.destroy();
                }
                logger.info("create new TAtomDsConfHandle dbName : " + dbName);
            } else {
                dsConfHandle = cacheConfHandle;
                logger.info("use the cache TAtomDsConfHandle dbName : " + dbName);
            }

            if (appConfigChangeListener != null) {
                String listenerName = String.format("%s-%s-%s",
                    this.getAppName(),
                    groupKey,
                    Math.abs(hashCode()));
                dsConfHandle.addAtomAppConfigChangeListener(listenerName, appConfigChangeListener);
            }

            // 添加一个引用声明
            dsConfHandle.addDataSourceReferences(dbName + "#@#" + groupKey);
        }
    }

    /**
     * 清除掉所有数据源
     */
    public static void cleanAllDataSource() {
        synchronized (cacheConfHandleMap) {
            for (TAtomDsConfHandle handles : cacheConfHandleMap.values()) {
                try {
                    handles.destroyDataSource();
                } catch (Exception e) {
                    logger.info("destroy TAtomDsConfHandle failed!", e);
                }
            }
            cacheConfHandleMap.clear();
        }
    }

    /**
     * 刷新数据源
     */
    @Override
    public void flushDataSource() {
        this.dsConfHandle.flushDataSource();
    }

    @Override
    protected void doDestroy() {
        String dbName = TAtomConstants.getDbNameStr(this.getUnitName(), this.getAppName(), this.getDbKey());
        synchronized (cacheConfHandleMap) {
            this.dsConfHandle.removeDataSourceReferences(dbName + "#@#" + groupKey);
            if (this.dsConfHandle.getDataSourceReferences().isEmpty()) {
                // 没有引用之后才进行关闭数据源
                this.dsConfHandle.destroyDataSource();
                cacheConfHandleMap.remove(dbName);
            }
        }
    }

    /**
     * 销毁数据源，慎用
     */
    @Override
    public void destroyDataSource() throws Exception {
        destroy();
    }

    public String getAppName() {
        return this.dsConfHandle.getAppName();
    }

    public String getDbKey() {
        return this.dsConfHandle.getDbKey();
    }

    public void setAppName(String appName) {
        this.dsConfHandle.setAppName(TStringUtil.trim(appName));
    }

    public void setDbKey(String dbKey) {
        this.dsConfHandle.setDbKey(TStringUtil.trim(dbKey));
    }

    public void setUnitName(String unitName) {
        this.dsConfHandle.setUnitName(TStringUtil.trim(unitName));
    }

    public String getUnitName() {
        return this.dsConfHandle.getUnitName();
    }

    @Override
    public TAtomDbStatusEnum getDbStatus() {
        return this.dsConfHandle.getStatus();
    }

    public void setDbStatusListeners(List<AtomDbStatusListener> dbStatusListeners) {
        this.dsConfHandle.setDbStatusListeners(dbStatusListeners);
    }

    public void setSingleInGroup(boolean isSingleInGroup) {
        this.dsConfHandle.setSingleInGroup(isSingleInGroup);
    }

    /**
     * =======以下是设置本地优先的配置属性，如果设置了会忽略推送的配置而使用本地的配置=======
     */
    public void setPasswd(String passwd) {
        this.dsConfHandle.setLocalPasswd(passwd);
    }

    public void setDriverClass(String driverClass) {
        this.dsConfHandle.setLocalDriverClass(driverClass);
    }

    public void setSorterClass(String sorterClass) {
        this.dsConfHandle.setLocalSorterClass(sorterClass);
    }

    public void setConnectionProperties(Map<String, String> map) {
        this.dsConfHandle.setLocalConnectionProperties(map);
    }

    @Override
    public DataSource getDataSource() {
        return this.dsConfHandle.getDataSource();
    }

    @Override
    public Connection getConnection() throws SQLException {
        final Connection connection = this.dsConfHandle.getDataSource().getConnection();
        if (connection.isWrapperFor(XConnection.class)) {
            // Optimize. Do not restore to default encoding.
            connection.unwrap(XConnection.class).getSession().setDefalutEncodingMySQL(null);
        }
        return connection;
    }

    public String getHost() {
        return this.dsConfHandle.getRunTimeConf().getIp();
    }

    public String getPort() {
        return this.dsConfHandle.getRunTimeConf().getPort();
    }

    public String getDsMode() {

        if (dsModeInGroupKey != null) {
            return dsModeInGroupKey;
        }

        return this.dsConfHandle.getRunTimeConf().getDsMode();
    }

    public TAtomDsConfHandle getDsConfHandle() {
        return dsConfHandle;
    }

    public AtomAppConfigChangeListener getAppConfigChangeListener() {
        return appConfigChangeListener;
    }

    public void setAppConfigChangeListener(AtomAppConfigChangeListener appConfigChangeListener) {
        this.appConfigChangeListener = appConfigChangeListener;
        if (appConfigChangeListener != null) {
            String listenerName = String.format("%s-%s-%s",
                this.getAppName(),
                groupKey,
                Math.abs(hashCode()));

            dsConfHandle.addAtomAppConfigChangeListener(listenerName, appConfigChangeListener);
        }
    }

    public String getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(String groupKey) {
        this.groupKey = groupKey;
    }

    public void setDsModeInGroupKey(String dsModeInGroupKey) {
        this.dsModeInGroupKey = dsModeInGroupKey;
    }

    public boolean isMasterDB() {
        return masterDB;
    }

    public String getDnId() {
        if (dnId == null) {
            return IDataSource.EMPTY;
        }
        return dnId;
    }

    public void setDnId(String dnId) {
        this.dnId = dnId;
    }
}
