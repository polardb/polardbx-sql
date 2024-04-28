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

package com.alibaba.polardbx.gms.config.impl;

import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.config.InstConfigManager;
import com.alibaba.polardbx.gms.config.InstConfigReceiver;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.InstConfigAccessor;
import com.alibaba.polardbx.gms.topology.InstConfigRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author chenghui.lch
 */
public class MetaDbInstConfigManager extends AbstractLifecycle implements InstConfigManager {

    /**
     * <pre>
     *     key: param key
     *     val: param val (string)
     * </pre>
     */
    protected volatile Properties propertiesInfoMap = new Properties();

    /**
     * <pre>
     *     key: dbname (lowercase)
     *     val: the db config receiver of one db
     * </pre>
     */
    protected Map<String, InstConfigReceiver> dbConfigReceiverMap = new ConcurrentHashMap<>();
    protected InstConfigReceiver instConfigReceiver;
    protected static MetaDbInstConfigManager instance = new MetaDbInstConfigManager();
    private static volatile boolean configFromMetaDb = true;

    public static MetaDbInstConfigManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }

    protected MetaDbInstConfigManager() {
    }

    @Override
    protected void doInit() {
        SystemDefaultPropertyHelper.initDefaultInstConfig();
        Properties newProps = null;
        if (configFromMetaDb) {
            try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
                newProps = loadPropertiesFromMetaDbConn(conn);
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }
        } else {
            newProps = new Properties();
        }
        this.propertiesInfoMap = newProps;
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
    }

    @Override
    public void registerDbReceiver(String dbName, InstConfigReceiver receiver) {
        synchronized (this) {
            if (receiver != null) {
                receiver.apply(this.propertiesInfoMap);
            }
            dbConfigReceiverMap.putIfAbsent(dbName.toLowerCase(), receiver);
        }
    }

    @Override
    public void unRegisterDbReceiver(String dbName) {
        synchronized (this) {
            dbConfigReceiverMap.remove(dbName.toLowerCase());
        }
    }

    @Override
    public void registerInstReceiver(InstConfigReceiver instReceiver) {
        synchronized (this) {
            if (instReceiver != null) {
                instReceiver.apply(this.propertiesInfoMap);
            }
            this.instConfigReceiver = instReceiver;
        }

    }

    @Override
    public void reloadInstConfig() {

        synchronized (this) {
            try {

                Properties newProps = null;
                // reload new properties
                try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
                    newProps = loadPropertiesFromMetaDbConn(conn);
                } catch (Throwable ex) {
                    throw GeneralUtil.nestedException(ex);
                }
                this.propertiesInfoMap = newProps;

                // Apply new configs for Cluster
                if (instConfigReceiver != null) {
                    instConfigReceiver.apply(this.propertiesInfoMap);
                }

                // Apply new configs for all dbs
                for (Map.Entry<String, InstConfigReceiver> dbReceiverItem : dbConfigReceiverMap.entrySet()) {
                    InstConfigReceiver configReceiver = dbReceiverItem.getValue();
                    if (configReceiver != null) {
                        configReceiver.apply(this.propertiesInfoMap);
                    }
                }

                // handle conn pool config
                processConnPoolConfig(this.propertiesInfoMap);

            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }
        }
    }

    protected void processConnPoolConfig(Properties props) {
        ConnPoolConfigManager.getInstance().refreshConnPoolConfig(props);
    }

    public static Properties loadPropertiesFromMetaDbConn(Connection conn) {

        Properties props = new Properties();
        try {
            InstConfigAccessor instConfigAcc = new InstConfigAccessor();
            instConfigAcc.setConnection(conn);
            String instId = InstIdUtil.getInstId();
            List<InstConfigRecord> instConfigs = instConfigAcc.getAllInstConfigsByInstId(instId);
            for (int i = 0; i < instConfigs.size(); i++) {
                props.put(instConfigs.get(i).paramKey, instConfigs.get(i).paramVal);
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
        return props;
    }

    public Properties getCnVariableConfigMap() {
        return propertiesInfoMap;
    }

    public static String getOriginalName(String paramName) {
        if (paramName.equalsIgnoreCase(ConnectionProperties.TRANSACTION_POLICY)) {
            return TransactionAttribute.DRDS_TRANSACTION_POLICY;
        }
        return paramName.toLowerCase(Locale.ROOT);
    }

    public String getInstProperty(String propKey) {
        String propVal = this.propertiesInfoMap.getProperty(propKey);
        return propVal;
    }

    public String getInstProperty(String propKey, String defaultValue) {
        String value = this.propertiesInfoMap.getProperty(propKey);
        if (StringUtils.isNotEmpty(value)) {
            return value;
        }
        return defaultValue;
    }

    public static void setConfigFromMetaDb(boolean configFromMetaDb) {
        MetaDbInstConfigManager.configFromMetaDb = configFromMetaDb;
    }
}
